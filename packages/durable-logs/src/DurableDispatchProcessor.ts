/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { diag } from '@opentelemetry/api';
import type { Context, HrTime } from '@opentelemetry/api';
import type {
  ForceFlushOptions,
  LogRecordProcessor,
  ReadWriteLogRecord,
} from '@opentelemetry/sdk-logs';
import type { ExporterRegistry } from './ExporterRegistry';
import { DELIVERY_PLAN_KEY } from './internal/deliveryPlan';
import type { DeliveryPlan } from './internal/deliveryPlan';
import type { QueueStorage } from './storage/QueueStorage';
import { targetKey, validatePolicy } from './storage/StateQueueStorage';
import type {
  ConfigDecision,
  DeliveryPolicy,
  ExporterConfigSource,
  ExporterRegistration,
  LogRecordSnapshot,
  TargetRef,
} from './types';

function copyTime(value: HrTime): HrTime {
  return [value[0], value[1]];
}

function snapshot(record: ReadWriteLogRecord): LogRecordSnapshot {
  const attributes = structuredClone(record.attributes);
  const eventId = attributes['event.id'];
  if (typeof eventId !== 'string' || !eventId)
    throw new Error('missing event.id');
  return {
    eventId,
    eventName: record.eventName,
    hrTime: copyTime(record.hrTime),
    hrTimeObserved: copyTime(record.hrTimeObserved),
    spanContext:
      record.spanContext === undefined
        ? undefined
        : structuredClone(record.spanContext),
    attributes,
    body: record.body === undefined ? undefined : structuredClone(record.body),
    severityNumber: record.severityNumber,
    severityText: record.severityText,
    droppedAttributesCount: record.droppedAttributesCount,
    resourceAttributes: structuredClone(record.resource.attributes),
    resourceSchemaUrl: record.resource.schemaUrl,
    instrumentationScope: structuredClone(record.instrumentationScope),
  };
}

export interface DurableDispatchOptions {
  storage: QueueStorage;
  registry: ExporterRegistry;
  isOnline?: () => boolean;
  subscribeOnline?: (wake: () => void) => () => void;
  now?: () => number;
}

/** One processor owns persistence and per-target asynchronous scheduling. */
export class DurableDispatchProcessor implements LogRecordProcessor {
  private state:
    | 'new'
    | 'starting'
    | 'running'
    | 'draining'
    | 'stopping'
    | 'stopped' = 'new';
  private starting?: Promise<void>;
  private stopping?: Promise<void>;
  private readonly pending = new Set<Promise<unknown>>();
  private readonly drives = new Map<string, Promise<void>>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly refreshTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();
  private readonly refreshing = new Set<string>();
  private readonly pendingWake = new Set<string>();
  private readonly pendingForce = new Set<string>();
  private readonly decisions = new Map<string, Map<string, ConfigDecision>>();
  private readonly policies = new Map<string, DeliveryPolicy>();
  private readonly policyRevisions = new Map<string, number>();
  private readonly unsubscribe: Array<() => void> = [];
  private readonly now: () => number;
  private readonly options: DurableDispatchOptions;

  constructor(options: DurableDispatchOptions) {
    this.options = options;
    this.now = options.now ?? Date.now;
  }

  start(): Promise<void> {
    if (this.state === 'running') return Promise.resolve();
    if (this.starting) return this.starting;
    if (
      this.state === 'draining' ||
      this.state === 'stopping' ||
      this.state === 'stopped'
    )
      return Promise.reject(new Error('processor has shut down'));
    this.state = 'starting';
    this.starting = this.initialize()
      .catch(async error => {
        this.cleanup();
        this.state = 'stopped';
        await this.options.storage.close();
        throw error;
      })
      .finally(() => {
        this.starting = undefined;
      });
    return this.starting;
  }

  private async initialize(): Promise<void> {
    await this.options.storage.open();
    const refreshes: Array<{
      target: TargetRef;
      source: ExporterConfigSource;
      delay?: number;
    }> = [];
    const subscriptions: Array<{
      target: TargetRef;
      source: ExporterConfigSource;
    }> = [];
    for (const registration of this.options.registry.getRegistrations()) {
      const target = { id: registration.id, targetKey: registration.targetKey };
      const key = targetKey(target);
      if (!registration.deliveryPolicy)
        throw new Error(
          `durable exporter ${registration.id} needs deliveryPolicy`
        );
      validatePolicy(registration.deliveryPolicy);
      if (registration.deliveryPolicySource?.subscribe) {
        this.unsubscribe.push(
          registration.deliveryPolicySource.subscribe(() => {
            void this.refreshPolicy(registration);
          })
        );
      }
      await this.refreshPolicy(registration);
      const decisions = new Map<string, ConfigDecision>();
      for (const source of registration.configSources ?? []) {
        const cached = await this.options.storage.getConfig(target, source.id);
        decisions.set(source.id, cached?.decision ?? source.fallback);
        let delay: number | undefined = Math.max(
          0,
          (cached?.refreshAt ?? 0) - this.now()
        );
        if (source.initialLoad === 'blocking') {
          try {
            const loaded = await source.load();
            if (typeof loaded.enabled !== 'boolean')
              throw new Error('config source returned an invalid decision');
            const current = decisions.get(source.id) ?? source.fallback;
            const decision = this.isStale(current, loaded) ? current : loaded;
            decisions.set(source.id, decision);
            delay = this.refreshDelay(source, decision);
            await this.options.storage.setConfig(target, source.id, {
              decision,
              refreshAt: delay === undefined ? 0 : this.now() + delay,
            });
          } catch (error) {
            diag.warn('durable log initial config load failed', error);
            delay =
              source.failureRefreshAfterMs ?? source.fallback.refreshAfterMs;
          }
        }
        if (source.subscribe) subscriptions.push({ target, source });
        refreshes.push({
          target,
          source,
          delay,
        });
      }
      this.decisions.set(key, decisions);
    }
    this.unsubscribe.push(
      this.options.registry.subscribeAvailability(target => this.wake(target))
    );
    if (this.options.storage.subscribeChanges) {
      this.unsubscribe.push(
        this.options.storage.subscribeChanges(target => this.wake(target))
      );
    }
    if (this.options.subscribeOnline) {
      this.unsubscribe.push(this.options.subscribeOnline(() => this.wakeAll()));
    }
    this.state = 'running';
    for (const { target, source } of subscriptions) {
      this.unsubscribe.push(
        source.subscribe!(() => this.scheduleRefresh(target, source, 0))
      );
    }
    for (const { target, source, delay } of refreshes) {
      if (delay !== undefined) this.scheduleRefresh(target, source, delay);
    }
    this.wakeAll();
  }

  onEmit(record: ReadWriteLogRecord, context?: Context): void {
    const plan = context?.getValue(DELIVERY_PLAN_KEY) as
      | DeliveryPlan
      | undefined;
    if (!plan) return;
    if (plan.claimed) {
      plan.reject(new Error('delivery plan was already consumed'));
      return;
    }
    plan.claimed = true;
    if (this.state !== 'running') {
      plan.reject(new Error('processor has not started'));
      return;
    }
    let copied: LogRecordSnapshot;
    try {
      copied = snapshot(record);
      if (copied.eventId !== plan.eventId)
        throw new Error('event.id changed before durable delivery');
    } catch (error) {
      plan.reject(error);
      return;
    }

    const available: TargetRef[] = [];
    const suppressed: Array<{ exporterId: string; reason: string }> = [];
    for (const target of plan.available) {
      const decision = this.decision(target);
      if (
        !decision.enabled ||
        decision.disabledEvents.has(copied.eventName ?? '')
      ) {
        suppressed.push({ exporterId: target.id, reason: 'configuration' });
      } else available.push(target);
    }
    if (!available.length) {
      plan.resolve({
        eventId: plan.eventId,
        matched: plan.matched,
        unavailable: plan.unavailable,
        queued: [],
        suppressed,
        status: 'suppressed',
      });
      return;
    }

    // The synchronous OTel hook only snapshots. One async transaction commits all targets.
    const task = Promise.resolve()
      .then(async () => {
        if (plan.cancelled) return;
        const policies = new Map<string, DeliveryPolicy>();
        const fallbacks = new Map<
          string,
          ReadonlyMap<string, ConfigDecision>
        >();
        for (const target of available) {
          const registration = this.options.registry.getInitializedTarget(
            target.id,
            target.targetKey
          );
          if (!registration)
            throw new Error('exporter became unavailable before persistence');
          policies.set(targetKey(target), this.policy(registration));
          fallbacks.set(
            targetKey(target),
            new Map(
              (registration.configSources ?? []).map(source => [
                source.id,
                source.fallback,
              ])
            )
          );
        }
        const deliveries = available.map(target => ({
          ...target,
          eventId: plan.eventId,
          snapshot: copied,
          enqueuedAt: this.now(),
        }));
        const result = await this.options.storage.appendFanout(
          deliveries,
          policies,
          fallbacks
        );
        const finalSuppressed = [
          ...suppressed,
          ...result.suppressed.map(target => ({
            exporterId: target.id,
            reason: 'configuration',
          })),
        ];
        plan.resolve({
          eventId: plan.eventId,
          matched: plan.matched,
          unavailable: plan.unavailable,
          queued: result.queued.map(target => target.id),
          suppressed: finalSuppressed,
          status: result.queued.length ? 'queued' : 'suppressed',
        });
        for (const target of result.queued) this.wake(target);
      })
      .catch(error => plan.reject(error))
      .finally(() => this.pending.delete(task));
    this.pending.add(task);
  }

  private decision(target: TargetRef): {
    enabled: boolean;
    disabledEvents: ReadonlySet<string>;
  } {
    const values = this.decisions.get(targetKey(target));
    if (!values) return { enabled: true, disabledEvents: new Set() };
    const disabledEvents = new Set<string>();
    let enabled = true;
    for (const value of values.values()) {
      enabled &&= value.enabled;
      for (const event of value.disabledEvents ?? []) disabledEvents.add(event);
    }
    return { enabled, disabledEvents };
  }

  private policy(registration: ExporterRegistration): DeliveryPolicy {
    const policy =
      this.policies.get(targetKey(registration)) ?? registration.deliveryPolicy;
    if (!policy) throw new Error('durable exporter needs deliveryPolicy');
    return policy;
  }

  private async refreshPolicy(
    registration: ExporterRegistration
  ): Promise<void> {
    const key = targetKey(registration);
    const source = registration.deliveryPolicySource;
    const fallback = registration.deliveryPolicy;
    if (!fallback) throw new Error('durable exporter needs deliveryPolicy');
    if (!source) {
      this.policies.set(key, fallback);
      return;
    }
    const revision = (this.policyRevisions.get(key) ?? 0) + 1;
    this.policyRevisions.set(key, revision);
    try {
      const overrides = await source.load();
      const policy = { ...fallback, ...overrides };
      validatePolicy(policy);
      if (this.policyRevisions.get(key) !== revision) return;
      this.policies.set(key, policy);
      this.wake(registration);
    } catch (error) {
      diag.warn('durable log delivery policy refresh failed', error);
    }
  }

  private scheduleRefresh(
    target: TargetRef,
    source: ExporterConfigSource,
    delay: number
  ): void {
    if (this.state !== 'running') return;
    const key = targetKey(target) + ':' + source.id;
    const old = this.refreshTimers.get(key);
    if (old) clearTimeout(old);
    this.refreshTimers.set(
      key,
      setTimeout(() => {
        void this.refresh(target, source);
      }, delay)
    );
  }

  private refreshDelay(
    source: ExporterConfigSource,
    decision: ConfigDecision
  ): number | undefined {
    const interval = decision.refreshAfterMs ?? source.fallback.refreshAfterMs;
    if (interval === undefined) return undefined;
    if (!Number.isFinite(interval) || interval <= 0)
      throw new TypeError('invalid refreshAfterMs');
    const base = Math.max(1000, interval);
    const jitter = source.refreshJitterRatio ?? 0;
    if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1)
      throw new TypeError('invalid refreshJitterRatio');
    return Math.max(1000, base * (1 + (Math.random() * 2 - 1) * jitter));
  }

  private isStale(current: ConfigDecision, incoming: ConfigDecision): boolean {
    return (
      current.version !== undefined &&
      (incoming.version === undefined || incoming.version <= current.version)
    );
  }

  private async refresh(
    target: TargetRef,
    source: ExporterConfigSource
  ): Promise<void> {
    const key = targetKey(target) + ':' + source.id;
    if (this.refreshing.has(key) || this.state !== 'running') return;
    this.refreshing.add(key);
    try {
      const loaded = await source.load();
      if (this.state !== 'running') return;
      if (typeof loaded.enabled !== 'boolean')
        throw new Error('config source returned an invalid decision');
      const current =
        this.decisions.get(targetKey(target))?.get(source.id) ??
        source.fallback;
      const decision = this.isStale(current, loaded) ? current : loaded;
      const delay = this.refreshDelay(source, decision);
      const refreshAt = delay === undefined ? 0 : this.now() + delay;
      await this.options.storage.setConfig(target, source.id, {
        decision,
        refreshAt,
      });
      this.decisions.get(targetKey(target))?.set(source.id, decision);
      this.wake(target);
      if (delay !== undefined) this.scheduleRefresh(target, source, delay);
    } catch (error) {
      diag.warn('durable log config refresh failed', error);
      const delay =
        source.failureRefreshAfterMs ?? source.fallback.refreshAfterMs;
      if (this.state === 'running' && delay !== undefined)
        this.scheduleRefresh(target, source, delay);
    } finally {
      this.refreshing.delete(key);
    }
  }

  private wakeAll(force = false): void {
    for (const registration of this.options.registry.getRegistrations()) {
      this.wake(
        { id: registration.id, targetKey: registration.targetKey },
        force
      );
    }
  }

  private wake(target: TargetRef, force = false): void {
    if (this.state !== 'running' && this.state !== 'draining') return;
    const key = targetKey(target);
    if (force) this.pendingForce.add(key);
    const timer = this.timers.get(key);
    if (timer) clearTimeout(timer);
    this.timers.delete(key);
    if (this.drives.has(key)) {
      this.pendingWake.add(key);
      return;
    }
    const drive = this.drive(target).catch(error => {
      diag.error('durable log dispatch failed', error);
      if (this.state === 'running' || this.state === 'draining')
        this.wakeAt(target, this.now() + 1000);
    });
    this.drives.set(key, drive);
    void drive.finally(() => {
      this.drives.delete(key);
      if (this.pendingWake.delete(key)) this.wake(target);
    });
  }

  private wakeAt(target: TargetRef, at: number): void {
    const key = targetKey(target);
    const delay = Math.max(1, Math.min(2147483647, at - this.now()));
    this.timers.set(
      key,
      setTimeout(() => this.wake(target), delay)
    );
  }

  private async drive(target: TargetRef): Promise<void> {
    const key = targetKey(target);
    for (;;) {
      if (this.state !== 'running' && this.state !== 'draining') return;
      const registration = this.options.registry.getInitializedTarget(
        target.id,
        target.targetKey
      );
      if (!registration || this.options.isOnline?.() === false) return;
      const decision = this.decision(target);
      if (!decision.enabled) return;
      const info = await this.options.storage.inspect(target);
      const head = info.head;
      if (!head) {
        this.pendingForce.delete(key);
        return;
      }
      const now = this.now();
      if (info.leaseUntil > now) {
        this.wakeAt(target, info.leaseUntil);
        return;
      }
      if (head.retryAt > now) {
        this.wakeAt(target, head.retryAt);
        return;
      }
      const policy = this.policy(registration);
      const force = this.pendingForce.has(key);
      const batchDue =
        force || info.drainActive || info.count >= policy.triggerCount
          ? now
          : head.enqueuedAt + policy.maxWaitMs;
      const due = Math.max(
        batchDue,
        info.lastTerminalAckAt + policy.minIntervalMs
      );
      if (due > now) {
        this.wakeAt(target, due);
        return;
      }
      const claim = await this.options.storage.claim(
        target,
        policy,
        now,
        !force,
        decision.disabledEvents,
        new Map(
          (registration.configSources ?? []).map(source => [
            source.id,
            source.fallback,
          ])
        )
      );
      if (!claim) {
        const after = await this.options.storage.inspect(target);
        if (!after.head) return;
        if (after.leaseUntil > now) {
          this.wakeAt(target, after.leaseUntil);
          return;
        }
        if (after.head.retryAt > now) {
          this.wakeAt(target, after.head.retryAt);
          return;
        }
        if (after.head.eventId === head.eventId) {
          this.wakeAt(target, now + 1000);
          return;
        }
        continue;
      }
      this.pendingForce.delete(key);
      let result;
      try {
        result = await registration.exporter.export(
          claim.entries.map(entry => entry.snapshot)
        );
      } catch {
        result = { kind: 'retry' as const };
      }
      const settled = await this.options.storage.settle(
        target,
        claim.token,
        result,
        policy,
        this.now()
      );
      if (!settled) continue; // Lease expired or another browser tab now owns it.
      if (
        result.kind !== 'items' ||
        result.items.some(item => item.status === 'retryable')
      ) {
        const after = await this.options.storage.inspect(target);
        if (after.head)
          this.wakeAt(target, Math.max(this.now() + 1, after.head.retryAt));
        return;
      }
    }
  }

  async forceFlush(options?: ForceFlushOptions): Promise<void> {
    if (this.state !== 'running' && this.state !== 'draining') return;
    const timeoutMillis = options?.timeoutMillis ?? 30000;
    if (
      !Number.isSafeInteger(timeoutMillis) ||
      timeoutMillis <= 0 ||
      timeoutMillis > 2147483647
    )
      throw new TypeError('timeoutMillis must be a positive timeout');
    const deadline = Date.now() + timeoutMillis;
    await this.waitWithin(Promise.all(this.pending), deadline);
    this.wakeAll(true);
    for (;;) {
      const targets = this.options.registry
        .getRegistrations()
        .map(({ id, targetKey: key }) => ({ id, targetKey: key }));
      const inspections = await this.waitWithin(
        Promise.all(
          targets.map(target => this.options.storage.inspect(target))
        ),
        deadline
      );
      const outstanding = targets.filter(
        (_, index) => inspections[index].count
      );
      if (outstanding.length === 0) return;
      if (
        this.options.isOnline?.() === false ||
        outstanding.some(
          target =>
            !this.options.registry.getInitializedTarget(
              target.id,
              target.targetKey
            ) || !this.decision(target).enabled
        )
      )
        throw new Error('durable log flush cannot dispatch queued records');
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error('durable log flush timed out');
      const active = [...this.drives.values()];
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          ...(active.length ? active : []),
          new Promise<void>(resolve => {
            timer = setTimeout(resolve, Math.min(remaining, 100));
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    }
  }

  private async waitWithin<T>(
    operation: Promise<T>,
    deadline: number
  ): Promise<T> {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error('durable log flush timed out');
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(
            () => reject(new Error('durable log flush timed out')),
            remaining
          );
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  shutdown(): Promise<void> {
    if (this.stopping) return this.stopping;
    this.stopping = this.stop();
    return this.stopping;
  }

  private async stop(): Promise<void> {
    if (this.state === 'stopped') return;
    if (this.starting) await this.starting;
    if (this.state === 'running') {
      this.state = 'draining';
      try {
        await this.forceFlush();
      } catch (error) {
        // The queue remains durable for the next process start.
        diag.warn('durable log shutdown flush failed', error);
      }
    }
    this.state = 'stopping';
    this.cleanup();
    await Promise.all(this.pending);
    await this.options.storage.close();
    this.state = 'stopped';
  }

  private cleanup(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    for (const timer of this.refreshTimers.values()) clearTimeout(timer);
    this.timers.clear();
    this.refreshTimers.clear();
    for (const unsubscribe of this.unsubscribe.splice(0)) unsubscribe();
  }
}
