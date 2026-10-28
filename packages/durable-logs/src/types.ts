/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  AnyValue,
  Attributes,
  Context,
  HrTime,
  SeverityNumber,
  SpanContext,
} from '@opentelemetry/api';

/** The caller's event before any OTel processor has changed it. */
export interface PreparedLogRecord {
  readonly eventName: string;
  readonly timestamp: number;
  readonly attributes: Readonly<Attributes>;
  readonly body?: AnyValue;
}

export interface EmitInput {
  eventName: string;
  timestamp?: number;
  context?: Context;
  attributes?: Attributes;
  body?: AnyValue;
  severityNumber?: SeverityNumber;
  severityText?: string;
}

export interface EmitReceipt {
  eventId: string;
  matched: string[];
  unavailable: string[];
  queued: string[];
  suppressed: Array<{ exporterId: string; reason: string }>;
  status: 'queued' | 'suppressed' | 'unmatched' | 'unavailable';
}

/** A serializable copy, detached from the SDK's mutable ReadWriteLogRecord. */
export interface LogRecordSnapshot {
  eventId: string;
  eventName?: string;
  hrTime: HrTime;
  hrTimeObserved: HrTime;
  spanContext?: SpanContext;
  attributes: Attributes;
  body?: AnyValue;
  severityNumber?: SeverityNumber;
  severityText?: string;
  droppedAttributesCount: number;
  resourceAttributes: Attributes;
  resourceSchemaUrl?: string;
  instrumentationScope: {
    name: string;
    version?: string;
    schemaUrl?: string;
    attributes?: Attributes;
  };
}

export type BatchResult =
  | {
      kind: 'items';
      items: readonly {
        eventId: string;
        status: 'accepted' | 'retryable' | 'rejected';
      }[];
      retryAfterMs?: number;
    }
  | { kind: 'retry' | 'pause'; retryAfterMs?: number };

export interface DurableLogExporter {
  initialize(): Promise<void>;
  export(records: readonly LogRecordSnapshot[]): Promise<BatchResult>;
  shutdown(): Promise<void>;
}

export interface DeliveryPolicy {
  triggerCount: number;
  maxWaitMs: number;
  maxBatchCount: number;
  maxBatchBytes: number;
  minIntervalMs: number;
  retryBaseMs: number;
  retryMaxMs: number;
  retryJitterRatio: number;
  queueCapacity: number;
  retentionMs: number;
  leaseMs: number;
}

export interface ConfigDecision {
  enabled: boolean;
  disabledEvents?: readonly string[];
  version?: number;
  refreshAfterMs?: number;
}

export interface ExporterConfigSource {
  id: string;
  fallback: ConfigDecision;
  load(): Promise<ConfigDecision>;
  initialLoad?: 'blocking' | 'background';
  subscribe?(notify: () => void): () => void;
  refreshJitterRatio?: number;
  failureRefreshAfterMs?: number;
}

export interface ExporterRegistration {
  id: string;
  targetKey: string;
  filter(record: Readonly<PreparedLogRecord>): Promise<boolean>;
  exporter: DurableLogExporter;
  deliveryPolicy: DeliveryPolicy;
  configSources?: readonly ExporterConfigSource[];
}

export interface TargetRef {
  readonly id: string;
  readonly targetKey: string;
}
