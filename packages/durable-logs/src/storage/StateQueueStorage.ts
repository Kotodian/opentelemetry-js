/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  BatchResult,
  ConfigDecision,
  DeliveryPolicy,
  TargetRef,
} from '../types';
import type {
  AppendResult,
  QueueClaim,
  QueueEntry,
  QueueInspection,
  QueuedDelivery,
  QueueStorage,
  StoredConfig,
} from './QueueStorage';

export interface Partition {
  nextSequence: number;
  items: QueueEntry[];
  drainActive: boolean;
  lastTerminalAckAt: number;
  token: number;
  leaseUntil: number;
  leaseIds: string[];
  configs: Record<string, StoredConfig>;
  rejected: Array<{ eventId: string; at: number }>;
}

export interface QueueState {
  version: 1;
  partitions: Record<string, Partition>;
}

export function emptyState(): QueueState {
  return { version: 1, partitions: {} };
}
export function targetKey(target: TargetRef): string {
  return JSON.stringify([target.id, target.targetKey]);
}

export function emptyPartition(): Partition {
  return {
    nextSequence: 1,
    items: [],
    drainActive: false,
    lastTerminalAckAt: 0,
    token: 0,
    leaseUntil: 0,
    leaseIds: [],
    configs: {},
    rejected: [],
  };
}

function partition(
  parts: Map<string, Partition>,
  target: TargetRef
): Partition {
  const key = targetKey(target);
  let value = parts.get(key);
  if (!value) {
    value = emptyPartition();
    parts.set(key, value);
  }
  return value;
}

function validatePolicy(policy: DeliveryPolicy): void {
  for (const name of [
    'triggerCount',
    'maxBatchCount',
    'maxBatchBytes',
    'queueCapacity',
    'leaseMs',
  ] as const) {
    if (!Number.isSafeInteger(policy[name]) || policy[name] <= 0)
      throw new TypeError('invalid delivery policy: ' + name);
  }
  for (const name of [
    'maxWaitMs',
    'minIntervalMs',
    'retryBaseMs',
    'retryMaxMs',
    'retentionMs',
  ] as const) {
    if (!Number.isFinite(policy[name]) || policy[name] < 0)
      throw new TypeError('invalid delivery policy: ' + name);
  }
  if (policy.retryBaseMs <= 0 || policy.retryMaxMs < policy.retryBaseMs) {
    throw new TypeError('invalid retry interval');
  }
  if (
    !Number.isFinite(policy.retryJitterRatio) ||
    policy.retryJitterRatio < 0 ||
    policy.retryJitterRatio > 1
  )
    throw new TypeError('invalid retryJitterRatio');
}

function validateValue(
  value: unknown,
  seen = new WeakSet<object>(),
  depth = 0
): void {
  if (depth > 64) throw new TypeError('queue value is too deeply nested');
  if (
    value === null ||
    value === undefined ||
    typeof value === 'string' ||
    typeof value === 'boolean'
  )
    return;
  if (typeof value === 'number' && Number.isFinite(value)) return;
  if (typeof value !== 'object' || seen.has(value))
    throw new TypeError('queue value is not serializable');
  if (value instanceof Uint8Array) return;
  const prototype = Object.getPrototypeOf(value);
  if (
    !Array.isArray(value) &&
    prototype !== Object.prototype &&
    prototype !== null
  ) {
    throw new TypeError('queue value is not serializable');
  }
  seen.add(value);
  for (const item of Object.values(value)) validateValue(item, seen, depth + 1);
  seen.delete(value);
}

/** Shared transactional queue rules; adapters provide the atomic storage transaction. */
export abstract class StateQueueStorage implements QueueStorage {
  protected abstract readPart(
    target: TargetRef
  ): Promise<Partition | undefined>;
  protected abstract updateParts<T>(
    targets: readonly TargetRef[],
    mutate: (parts: Map<string, Partition>) => T
  ): Promise<T>;
  abstract open(): Promise<void>;
  abstract close(): Promise<void>;

  async appendFanout(
    deliveries: readonly QueuedDelivery[],
    policies: ReadonlyMap<string, DeliveryPolicy>,
    fallbacks: ReadonlyMap<
      string,
      ReadonlyMap<string, ConfigDecision>
    > = new Map()
  ): Promise<AppendResult> {
    if (deliveries.length === 0) return { queued: [], suppressed: [] };
    for (const delivery of deliveries) validateValue(delivery.snapshot);
    return this.updateParts(deliveries, parts => {
      const queued: TargetRef[] = [];
      const suppressed: TargetRef[] = [];
      for (const delivery of deliveries) {
        const key = targetKey(delivery);
        const policy = policies.get(key);
        if (!policy) throw new Error('missing delivery policy');
        validatePolicy(policy);
        const part = partition(parts, delivery);
        let allowed = true;
        for (const [sourceId, fallback] of fallbacks.get(key) ?? []) {
          const decision = part.configs[sourceId]?.decision ?? fallback;
          if (
            !decision.enabled ||
            decision.disabledEvents?.includes(delivery.snapshot.eventName ?? '')
          ) {
            allowed = false;
            break;
          }
        }
        if (!allowed) {
          suppressed.push(delivery);
          continue;
        }
        const same = part.items.find(item => item.eventId === delivery.eventId);
        if (same) {
          if (
            JSON.stringify(same.snapshot) !== JSON.stringify(delivery.snapshot)
          )
            throw new Error('event ID collision');
          queued.push(delivery);
          continue;
        }
        if (part.items.length >= policy.queueCapacity)
          throw new Error('queue capacity exceeded');
        part.items.push({
          ...delivery,
          sequence: part.nextSequence++,
          attempts: 0,
          retryAt: 0,
        });
        queued.push(delivery);
      }
      return { queued, suppressed };
    });
  }

  async inspect(target: TargetRef): Promise<QueueInspection> {
    const part = await this.readPart(target);
    return part
      ? {
          count: part.items.length,
          head: part.items[0],
          drainActive: part.drainActive,
          lastTerminalAckAt: part.lastTerminalAckAt,
          leaseUntil: part.leaseUntil,
        }
      : { count: 0, drainActive: false, lastTerminalAckAt: 0, leaseUntil: 0 };
  }

  async claim(
    target: TargetRef,
    policy: DeliveryPolicy,
    now: number,
    activateDrain: boolean,
    disabledEvents: ReadonlySet<string>,
    fallbacks: ReadonlyMap<string, ConfigDecision> = new Map()
  ): Promise<QueueClaim | undefined> {
    return this.updateParts([target], parts => {
      const part = partition(parts, target);
      const effectiveDisabled = new Set(disabledEvents);
      for (const [sourceId, fallback] of fallbacks) {
        const current = part.configs[sourceId]?.decision ?? fallback;
        if (!current.enabled) return undefined;
        for (const eventName of current.disabledEvents ?? [])
          effectiveDisabled.add(eventName);
      }
      if (part.leaseUntil > now) return undefined;
      // Expired leases can be reclaimed with a new fencing token.
      part.leaseIds = [];
      part.leaseUntil = 0;
      part.items = part.items.filter(item => {
        if (
          now - item.enqueuedAt <= policy.retentionMs &&
          !effectiveDisabled.has(item.snapshot.eventName ?? '')
        )
          return true;
        part.rejected.push({ eventId: item.eventId, at: now });
        return false;
      });
      part.rejected = part.rejected.slice(-policy.queueCapacity);
      if (!part.items.length) {
        part.drainActive = false;
        return undefined;
      }
      if (part.items[0].retryAt > now) return undefined;
      const batch: QueueEntry[] = [];
      let bytes = 0;
      for (const item of part.items) {
        if (item.retryAt > now || batch.length >= policy.maxBatchCount) break;
        const size = new TextEncoder().encode(
          JSON.stringify(item.snapshot)
        ).byteLength;
        if (size > policy.maxBatchBytes && batch.length === 0) {
          part.items.shift();
          part.rejected.push({ eventId: item.eventId, at: now });
          break;
        }
        if (bytes + size > policy.maxBatchBytes) break;
        batch.push(item);
        bytes += size;
      }
      if (!batch.length) return undefined;
      if (activateDrain) part.drainActive = true;
      part.token++;
      part.leaseUntil = now + policy.leaseMs;
      part.leaseIds = batch.map(item => item.eventId);
      return { token: part.token, entries: batch };
    });
  }

  async settle(
    target: TargetRef,
    token: number,
    result: BatchResult,
    policy: DeliveryPolicy,
    now: number
  ): Promise<boolean> {
    return this.updateParts([target], parts => {
      const part = partition(parts, target);
      if (
        part.token !== token ||
        part.leaseUntil < now ||
        part.leaseIds.length === 0
      )
        return false;
      const leased = new Set(part.leaseIds);
      const valid =
        result.kind === 'items' &&
        result.items.length === leased.size &&
        result.items.every(
          item =>
            leased.has(item.eventId) &&
            ['accepted', 'retryable', 'rejected'].includes(item.status)
        ) &&
        new Set(result.items.map(item => item.eventId)).size === leased.size;
      const statuses =
        valid && result.kind === 'items'
          ? new Map(result.items.map(item => [item.eventId, item.status]))
          : new Map<string, 'accepted' | 'retryable' | 'rejected'>();
      let retryable = false;
      for (const eventId of part.leaseIds) {
        const index = part.items.findIndex(item => item.eventId === eventId);
        if (index < 0) continue;
        const status = statuses.get(eventId) ?? 'retryable';
        if (status === 'accepted' || status === 'rejected') {
          part.items.splice(index, 1);
          if (status === 'rejected') part.rejected.push({ eventId, at: now });
        } else {
          const item = part.items[index];
          const attempts = item.attempts + 1;
          const exponential = Math.min(
            policy.retryMaxMs,
            policy.retryBaseMs * 2 ** Math.min(attempts - 1, 30)
          );
          const jitter = exponential * policy.retryJitterRatio * Math.random();
          const remoteDelay = result.retryAfterMs;
          const delay = Math.max(
            exponential + jitter,
            remoteDelay !== undefined &&
              Number.isFinite(remoteDelay) &&
              remoteDelay > 0
              ? remoteDelay
              : 0
          );
          part.items[index] = { ...item, attempts, retryAt: now + delay };
          retryable = true;
        }
      }
      part.rejected = part.rejected.slice(-policy.queueCapacity);
      part.leaseIds = [];
      part.leaseUntil = 0;
      if (!retryable) part.lastTerminalAckAt = now;
      if (!part.items.length) part.drainActive = false;
      return true;
    });
  }

  async getConfig(
    target: TargetRef,
    sourceId: string
  ): Promise<StoredConfig | undefined> {
    return (await this.readPart(target))?.configs[sourceId];
  }

  async setConfig(
    target: TargetRef,
    sourceId: string,
    config: StoredConfig
  ): Promise<void> {
    await this.updateParts([target], parts => {
      partition(parts, target).configs[sourceId] = config;
    });
  }
}
