/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type { TargetRef } from '../types';
import { StateQueueStorage, emptyState, targetKey } from './StateQueueStorage';
import type { Partition, QueueState } from './StateQueueStorage';

/** An explicit test adapter. Nothing written here survives process exit. */
export class MemoryQueueStorage extends StateQueueStorage {
  private state: QueueState = emptyState();
  private gate: Promise<void> = Promise.resolve();
  private opened = false;

  async open(): Promise<void> {
    this.opened = true;
  }

  protected async readPart(target: TargetRef): Promise<Partition | undefined> {
    if (!this.opened) throw new Error('queue storage is closed');
    await this.gate;
    const part = this.state.partitions[targetKey(target)];
    return part ? structuredClone(part) : undefined;
  }

  protected updateParts<T>(
    targets: readonly TargetRef[],
    mutate: (parts: Map<string, Partition>) => T
  ): Promise<T> {
    if (!this.opened)
      return Promise.reject(new Error('queue storage is closed'));
    const result = this.gate.then(() => {
      const parts = new Map<string, Partition>();
      for (const target of targets) {
        const key = targetKey(target);
        const part = this.state.partitions[key];
        if (part) parts.set(key, structuredClone(part));
      }
      const value = mutate(parts);
      for (const [key, part] of parts) this.state.partitions[key] = part;
      return value;
    });
    this.gate = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  async close(): Promise<void> {
    await this.gate;
    this.opened = false;
  }
}
