/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  BatchResult,
  ConfigDecision,
  DeliveryPolicy,
  LogRecordSnapshot,
  TargetRef,
} from '../types';

export interface QueuedDelivery extends TargetRef {
  readonly eventId: string;
  readonly snapshot: LogRecordSnapshot;
  readonly enqueuedAt: number;
}

export interface StoredConfig {
  readonly decision: ConfigDecision;
  readonly refreshAt: number;
}

export interface QueueEntry extends QueuedDelivery {
  readonly sequence: number;
  readonly attempts: number;
  readonly retryAt: number;
}

export interface QueueInspection {
  readonly count: number;
  readonly head?: QueueEntry;
  readonly drainActive: boolean;
  readonly lastTerminalAckAt: number;
  readonly leaseUntil: number;
}

export interface QueueClaim {
  readonly token: number;
  readonly entries: readonly QueueEntry[];
}

export interface AppendResult {
  readonly queued: readonly TargetRef[];
  readonly suppressed: readonly TargetRef[];
}

/** appendFanout commits every target, or none, before resolving. */
export interface QueueStorage {
  open(): Promise<void>;
  appendFanout(
    deliveries: readonly QueuedDelivery[],
    policies: ReadonlyMap<string, DeliveryPolicy>,
    fallbacks?: ReadonlyMap<string, ReadonlyMap<string, ConfigDecision>>
  ): Promise<AppendResult>;
  inspect(target: TargetRef): Promise<QueueInspection>;
  claim(
    target: TargetRef,
    policy: DeliveryPolicy,
    now: number,
    activateDrain: boolean,
    disabledEvents: ReadonlySet<string>,
    fallbacks?: ReadonlyMap<string, ConfigDecision>
  ): Promise<QueueClaim | undefined>;
  settle(
    target: TargetRef,
    token: number,
    result: BatchResult,
    policy: DeliveryPolicy,
    now: number
  ): Promise<boolean>;
  getConfig(
    target: TargetRef,
    sourceId: string
  ): Promise<StoredConfig | undefined>;
  setConfig(
    target: TargetRef,
    sourceId: string,
    config: StoredConfig
  ): Promise<void>;
  subscribeChanges?(notify: (target: TargetRef) => void): () => void;
  close(): Promise<void>;
}
