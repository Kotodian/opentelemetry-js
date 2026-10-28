/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { diag } from '@opentelemetry/api';
import type { ExporterDefinition } from './createTelemetry';
import type { ConfigDecision, ExporterConfigSource } from './types';

/** In-memory config decisions for Batch; Durable owns its persisted decisions. */
export class BatchConfigState {
  private readonly decisions = new Map<string, Map<string, ConfigDecision>>();
  private readonly timers = new Map<
    ExporterConfigSource,
    ReturnType<typeof setTimeout>
  >();
  private readonly unsubscribe: Array<() => void> = [];
  private readonly definitions: readonly ExporterDefinition[];
  private stopped = false;

  constructor(definitions: readonly ExporterDefinition[]) {
    this.definitions = definitions;
  }

  async start(): Promise<void> {
    const initial: Promise<void>[] = [];
    for (const definition of this.definitions) {
      const values = new Map<string, ConfigDecision>();
      this.decisions.set(definition.id, values);
      for (const source of definition.configSources ?? []) {
        if (!source.id || values.has(source.id))
          throw new Error(`duplicate config source for ${definition.id}`);
        values.set(source.id, source.fallback);
        if (source.subscribe) {
          this.unsubscribe.push(
            source.subscribe(() => {
              void this.refresh(definition.id, source);
            })
          );
        }
        const load = this.refresh(definition.id, source);
        if (source.initialLoad === 'blocking') initial.push(load);
      }
    }
    await Promise.all(initial);
  }

  isAllowed(id: string, eventName: string): boolean {
    for (const decision of this.decisions.get(id)?.values() ?? []) {
      if (!decision.enabled || decision.disabledEvents?.includes(eventName))
        return false;
    }
    return true;
  }

  private async refresh(
    id: string,
    source: ExporterConfigSource
  ): Promise<void> {
    const previous = this.timers.get(source);
    if (previous) clearTimeout(previous);
    let delay = source.failureRefreshAfterMs ?? source.fallback.refreshAfterMs;
    try {
      const loaded = await source.load();
      if (typeof loaded.enabled !== 'boolean')
        throw new TypeError('invalid config decision');
      if (this.stopped) return;
      const values = this.decisions.get(id);
      const current = values?.get(source.id);
      if (
        loaded.version === undefined ||
        current?.version === undefined ||
        loaded.version >= current.version
      )
        values?.set(source.id, loaded);
      delay = loaded.refreshAfterMs ?? source.fallback.refreshAfterMs;
    } catch (error) {
      diag.warn(`batch config refresh failed for ${id}`, error);
    }
    if (
      !this.stopped &&
      delay !== undefined &&
      Number.isFinite(delay) &&
      delay > 0
    ) {
      this.timers.set(
        source,
        setTimeout(() => {
          void this.refresh(id, source);
        }, delay)
      );
    }
  }

  shutdown(): void {
    this.stopped = true;
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    for (const unsubscribe of this.unsubscribe.splice(0)) unsubscribe();
  }
}
