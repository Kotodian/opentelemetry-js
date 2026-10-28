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
  /** Submitted to the in-memory Batch processor; not proof that it was queued. */
  submitted?: string[];
  queued: string[];
  suppressed: Array<{ exporterId: string; reason: string }>;
  status: 'submitted' | 'queued' | 'suppressed' | 'unmatched' | 'unavailable';
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
      /** Preserve a server-directed delay when a partial batch was rate limited. */
      retryAfterExact?: boolean;
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
  /** Optional per-target failure ladder; the last entry repeats. */
  retryDelaysMs?: readonly number[];
  /** The legacy default adds jitter; symmetric jitter can be opted into. */
  retryJitterMode?: 'positive' | 'symmetric';
  queueCapacity: number;
  /** Optional high-watermark eviction, independent of the hard queue capacity. */
  queueEviction?: { highWatermark: number; fraction: number };
  retentionMs: number;
  leaseMs: number;
}

/** Async caller-owned policy source, e.g. a network-quality observer. */
export interface DeliveryPolicySource {
  load(): Promise<Partial<DeliveryPolicy>>;
  subscribe?(notify: () => void): () => void;
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
  /** Existing manual registrations use this; a ConfigSource uses one top-level selector. */
  filter?(record: Readonly<PreparedLogRecord>): Promise<boolean>;
  exporter: DurableLogExporter;
  deliveryPolicy?: DeliveryPolicy;
  deliveryPolicySource?: DeliveryPolicySource;
  configSources?: readonly ExporterConfigSource[];
}

export interface TargetRef {
  readonly id: string;
  readonly targetKey: string;
}
