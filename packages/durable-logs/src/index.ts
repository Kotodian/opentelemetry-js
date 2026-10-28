/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

export { ExporterRegistry } from './ExporterRegistry';
export { TelemetryEmitter } from './TelemetryEmitter';
export { DurableDispatchProcessor } from './DurableDispatchProcessor';
export type { DurableDispatchOptions } from './DurableDispatchProcessor';
export type { TargetSelection } from './ExporterRegistry';
export type {
  QueueStorage,
  QueuedDelivery,
  QueueEntry,
  QueueClaim,
  QueueInspection,
  StoredConfig,
  AppendResult,
} from './storage/QueueStorage';
export type {
  BatchResult,
  ConfigDecision,
  DeliveryPolicy,
  DurableLogExporter,
  EmitInput,
  EmitReceipt,
  ExporterConfigSource,
  ExporterRegistration,
  LogRecordSnapshot,
  PreparedLogRecord,
  TargetRef,
} from './types';
