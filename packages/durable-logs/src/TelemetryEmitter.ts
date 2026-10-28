/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { context } from '@opentelemetry/api';
import type { Logger } from '@opentelemetry/api';
import type { ExporterRegistry } from './ExporterRegistry';
import { createDeliveryPlan, DELIVERY_PLAN_KEY } from './internal/deliveryPlan';
import type { EmitInput, EmitReceipt, PreparedLogRecord } from './types';

function freezeValue<T>(value: T): T {
  if (value && typeof value === 'object' && !ArrayBuffer.isView(value)) {
    for (const member of Object.values(value)) freezeValue(member);
    Object.freeze(value);
  }
  return value;
}

/** Async admission and persistence receipt for OTel log events. */
export class TelemetryEmitter {
  private readonly logger: Logger;
  private readonly registry: ExporterRegistry;
  private readonly makeEventId: () => string;

  constructor(
    logger: Logger,
    registry: ExporterRegistry,
    makeEventId: () => string = () => globalThis.crypto.randomUUID()
  ) {
    this.logger = logger;
    this.registry = registry;
    this.makeEventId = makeEventId;
  }

  async emit(input: EmitInput): Promise<EmitReceipt> {
    if (!input.eventName) throw new TypeError('eventName is required');
    const timestamp = input.timestamp ?? Date.now();
    if (!Number.isFinite(timestamp) || timestamp < 0)
      throw new TypeError('timestamp must be a UNIX millisecond value');
    const eventId = this.makeEventId();
    if (!eventId) throw new Error('event ID factory returned an empty ID');
    const attributes = freezeValue(
      structuredClone({
        ...input.attributes,
        'event.name': input.eventName,
        'event.id': eventId,
      })
    );
    const body =
      input.body === undefined
        ? undefined
        : freezeValue(structuredClone(input.body));
    const prepared: PreparedLogRecord = Object.freeze({
      eventName: input.eventName,
      timestamp,
      attributes,
      body,
    });
    const selection = await this.registry.select(prepared);
    const base = {
      eventId,
      matched: selection.matched,
      unavailable: selection.unavailable,
      queued: [],
      suppressed: [],
    };
    if (selection.matched.length === 0) return { ...base, status: 'unmatched' };
    if (selection.available.length === 0)
      return { ...base, status: 'unavailable' };

    const plan = createDeliveryPlan(
      eventId,
      selection.matched,
      selection.unavailable,
      selection.available
    );
    const logContext = (input.context ?? context.active()).setValue(
      DELIVERY_PLAN_KEY,
      plan
    );
    if (
      !this.logger.enabled({
        context: logContext,
        severityNumber: input.severityNumber,
        eventName: input.eventName,
      })
    ) {
      throw new Error('logger is disabled for an admitted event');
    }
    try {
      this.logger.emit({
        eventName: input.eventName,
        timestamp,
        body,
        attributes,
        context: logContext,
        severityNumber: input.severityNumber,
        severityText: input.severityText,
      });
    } catch (error) {
      plan.cancelled = true;
      if (plan.claimed) {
        plan.reject(error);
        return plan.result;
      }
      throw error;
    }
    if (!plan.claimed)
      throw new Error('admitted event did not reach DurableDispatchProcessor');
    return plan.result;
  }
}
