/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { diag } from '@opentelemetry/api';
import type { Context } from '@opentelemetry/api';
import { ExportResultCode } from '@opentelemetry/core';
import { BatchLogRecordProcessor } from '@opentelemetry/sdk-logs';
import type {
  BatchLogRecordProcessorOptions,
  LogRecordExporter,
  ReadableLogRecord,
  ReadWriteLogRecord,
} from '@opentelemetry/sdk-logs';
import type { ExporterRegistry } from './ExporterRegistry';
import { DELIVERY_PLAN_KEY } from './internal/deliveryPlan';
import type { DeliveryPlan } from './internal/deliveryPlan';
import type { LogRecordSnapshot, TargetRef } from './types';

function snapshot(record: ReadableLogRecord): LogRecordSnapshot {
  const eventId = record.attributes['event.id'];
  if (typeof eventId !== 'string' || !eventId)
    throw new Error('batch record is missing event.id');
  return {
    eventId,
    eventName: record.eventName,
    hrTime: [record.hrTime[0], record.hrTime[1]],
    hrTimeObserved: [record.hrTimeObserved[0], record.hrTimeObserved[1]],
    spanContext: record.spanContext,
    attributes: structuredClone(record.attributes),
    body: record.body === undefined ? undefined : structuredClone(record.body),
    severityNumber: record.severityNumber,
    severityText: record.severityText,
    droppedAttributesCount: record.droppedAttributesCount,
    resourceAttributes: structuredClone(record.resource.attributes),
    resourceSchemaUrl: record.resource.schemaUrl,
    instrumentationScope: structuredClone(record.instrumentationScope),
  };
}

class RoutingExporter implements LogRecordExporter {
  private readonly routes: WeakMap<ReadableLogRecord, readonly TargetRef[]>;
  private readonly registry: ExporterRegistry;

  constructor(
    routes: WeakMap<ReadableLogRecord, readonly TargetRef[]>,
    registry: ExporterRegistry
  ) {
    this.routes = routes;
    this.registry = registry;
  }

  export(
    records: ReadableLogRecord[],
    callback: (result: { code: ExportResultCode; error?: Error }) => void
  ): void {
    void this.send(records).then(
      () => callback({ code: ExportResultCode.SUCCESS }),
      error =>
        callback({
          code: ExportResultCode.FAILED,
          error: error instanceof Error ? error : new Error(String(error)),
        })
    );
  }

  private async send(records: ReadableLogRecord[]): Promise<void> {
    const grouped = new Map<
      string,
      { targetKey: string; records: LogRecordSnapshot[] }
    >();
    for (const record of records) {
      const destinations = this.routes.get(record) ?? [];
      this.routes.delete(record);
      if (!destinations.length) continue;
      const copied = snapshot(record);
      for (const target of destinations) {
        if (!this.registry.canDeliver(target.id, copied.eventName ?? ''))
          continue;
        let group = grouped.get(target.id);
        if (!group) {
          group = { targetKey: target.targetKey, records: [] };
          grouped.set(target.id, group);
        }
        group.records.push(copied);
      }
    }
    const outcomes = await Promise.allSettled(
      [...grouped].map(async ([id, group]) => {
        const registered = this.registry.getInitializedTarget(
          id,
          group.targetKey
        );
        if (!registered)
          throw new Error(`batch exporter ${id} became unavailable`);
        const result = await registered.exporter.export(group.records);
        if (
          result.kind !== 'items' ||
          result.items.length !== group.records.length ||
          result.items.some(item => item.status !== 'accepted')
        )
          throw new Error(`batch exporter ${id} did not accept its full batch`);
      })
    );
    const failed = outcomes.find(outcome => outcome.status === 'rejected');
    if (failed?.status === 'rejected') throw failed.reason;
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

/** Exactly one upstream Batch processor, with SDK-internal per-record routing. */
export class BatchDispatchProcessor extends BatchLogRecordProcessor {
  private readonly routes: WeakMap<ReadableLogRecord, readonly TargetRef[]>;

  constructor(
    registry: ExporterRegistry,
    options: Omit<BatchLogRecordProcessorOptions, 'exporter'> = {}
  ) {
    const routes = new WeakMap<ReadableLogRecord, readonly TargetRef[]>();
    super({ ...options, exporter: new RoutingExporter(routes, registry) });
    this.routes = routes;
  }

  override onEmit(record: ReadWriteLogRecord, context?: Context): void {
    const plan = context?.getValue(DELIVERY_PLAN_KEY) as
      | DeliveryPlan
      | undefined;
    if (!plan) return;
    if (plan.claimed) {
      plan.reject(new Error('delivery plan was already consumed'));
      return;
    }
    plan.claimed = true;
    this.routes.set(record, plan.available);
    try {
      super.onEmit(record);
      plan.resolve({
        eventId: plan.eventId,
        matched: plan.matched,
        unavailable: plan.unavailable,
        submitted: plan.available.map(target => target.id),
        queued: [],
        suppressed: [],
        status: 'submitted',
      });
    } catch (error) {
      this.routes.delete(record);
      diag.error('batch log admission failed', error);
      plan.reject(error);
    }
  }
}
