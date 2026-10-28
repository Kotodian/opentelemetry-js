/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  ExporterRegistration,
  PreparedLogRecord,
  TargetRef,
} from './types';

export type ExporterFilter = (
  record: Readonly<PreparedLogRecord>
) => Promise<readonly string[]>;

export interface TargetSelection {
  matched: string[];
  unavailable: string[];
  suppressed: string[];
  available: TargetRef[];
}

interface RegisteredTarget {
  registration: ExporterRegistration;
  initialized: boolean;
}

/** Filters events before Logger.emit and tracks each exporter's initialization. */
export class ExporterRegistry {
  private readonly filterTimeoutMs: number;
  private readonly selector?: ExporterFilter;
  private readonly declaredIds?: ReadonlySet<string>;
  private readonly isAllowed?: (id: string, eventName: string) => boolean;
  private readonly targets = new Map<string, RegisteredTarget>();
  private readonly listeners = new Set<(target: TargetRef) => void>();
  private sealed = false;
  private initialization?: Promise<
    Array<{ id: string; initialized: boolean; error?: unknown }>
  >;

  constructor(
    filterTimeoutMs = 5000,
    selector?: ExporterFilter,
    declaredIds?: ReadonlySet<string>,
    isAllowed?: (id: string, eventName: string) => boolean
  ) {
    if (!Number.isFinite(filterTimeoutMs) || filterTimeoutMs <= 0) {
      throw new TypeError('filterTimeoutMs must be positive');
    }
    this.filterTimeoutMs = filterTimeoutMs;
    this.selector = selector;
    this.declaredIds = declaredIds;
    this.isAllowed = isAllowed;
  }

  register(registration: ExporterRegistration): void {
    if (this.sealed) throw new Error('exporter registration is closed');
    if (
      !registration.id ||
      !registration.targetKey ||
      (!this.selector && typeof registration.filter !== 'function')
    ) {
      throw new TypeError('exporter id, targetKey, and a filter are required');
    }
    if (this.targets.has(registration.id))
      throw new Error(`duplicate exporter id: ${registration.id}`);
    if (!registration.exporter) {
      throw new TypeError('exporter is required');
    }
    const ids = new Set<string>();
    for (const source of registration.configSources ?? []) {
      if (!source.id || ids.has(source.id))
        throw new Error('config source IDs must be unique per target');
      ids.add(source.id);
    }
    this.targets.set(registration.id, { registration, initialized: false });
  }

  initialize(): Promise<
    Array<{ id: string; initialized: boolean; error?: unknown }>
  > {
    if (this.initialization) return this.initialization;
    this.sealed = true;
    this.initialization = Promise.all(
      [...this.targets.values()].map(async target => {
        const { id, targetKey, exporter } = target.registration;
        try {
          await exporter.initialize();
          target.initialized = true;
          this.notify({ id, targetKey });
          return { id, initialized: true };
        } catch (error) {
          target.initialized = false;
          return { id, initialized: false, error };
        }
      })
    );
    return this.initialization;
  }

  async retryInitialize(id: string): Promise<boolean> {
    if (!this.sealed) throw new Error('registry has not been initialized');
    const target = this.targets.get(id);
    if (!target) throw new Error(`unknown exporter id: ${id}`);
    if (target.initialized) return true;
    try {
      await target.registration.exporter.initialize();
      target.initialized = true;
      this.notify({ id, targetKey: target.registration.targetKey });
      return true;
    } catch {
      return false;
    }
  }

  async select(record: Readonly<PreparedLogRecord>): Promise<TargetSelection> {
    if (this.selector) {
      const ids = await this.withFilterTimeout(this.selector(record));
      if (!Array.isArray(ids))
        throw new TypeError('filter must return target IDs');
      const unique = new Set(ids);
      if (
        unique.size !== ids.length ||
        ids.some(id => !(this.declaredIds?.has(id) ?? this.targets.has(id)))
      )
        throw new Error('filter selected a duplicate or unknown exporter');
      const selected: TargetSelection = {
        matched: [],
        unavailable: [],
        suppressed: [],
        available: [],
      };
      for (const id of ids) {
        const target = this.targets.get(id);
        selected.matched.push(id);
        if (!target?.initialized) selected.unavailable.push(id);
        else if (this.isAllowed && !this.isAllowed(id, record.eventName))
          selected.suppressed.push(id);
        else
          selected.available.push({
            id,
            targetKey: target.registration.targetKey,
          });
      }
      return selected;
    }
    // Promise.all rejects the entire admission if any caller-supplied filter fails.
    const matches = await Promise.all(
      [...this.targets.values()].map(async target => ({
        target,
        matches: await this.runFilter(target.registration, record),
      }))
    );
    const selected: TargetSelection = {
      matched: [],
      unavailable: [],
      suppressed: [],
      available: [],
    };
    for (const { target, matches: hit } of matches) {
      if (!hit) continue;
      const { id, targetKey } = target.registration;
      selected.matched.push(id);
      if (target.initialized) selected.available.push({ id, targetKey });
      else selected.unavailable.push(id);
    }
    return selected;
  }

  getInitializedTarget(
    id: string,
    targetKey: string
  ): ExporterRegistration | undefined {
    const target = this.targets.get(id);
    return target?.initialized && target.registration.targetKey === targetKey
      ? target.registration
      : undefined;
  }

  canDeliver(id: string, eventName: string): boolean {
    return this.isAllowed?.(id, eventName) ?? true;
  }

  getRegistrations(): readonly ExporterRegistration[] {
    return [...this.targets.values()].map(target => target.registration);
  }

  subscribeAvailability(listener: (target: TargetRef) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async shutdown(): Promise<void> {
    await Promise.all(
      [...this.targets.values()].map(async target => {
        if (!target.initialized) return;
        target.initialized = false;
        this.notify({
          id: target.registration.id,
          targetKey: target.registration.targetKey,
        });
        await target.registration.exporter.shutdown();
      })
    );
  }

  private notify(target: TargetRef): void {
    for (const listener of this.listeners) {
      try {
        listener(target);
      } catch {
        /* one observer cannot change target readiness */
      }
    }
  }

  private async runFilter(
    registration: ExporterRegistration,
    record: Readonly<PreparedLogRecord>
  ): Promise<boolean> {
    return this.withFilterTimeout(registration.filter!(record));
  }

  private async withFilterTimeout<T>(filterResult: Promise<T>): Promise<T> {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        filterResult,
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(
            () => reject(new Error('exporter filter timed out')),
            this.filterTimeoutMs
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }
}
