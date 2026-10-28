/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type { TargetRef } from '../types';
import { StateQueueStorage, targetKey } from './StateQueueStorage';
import type { Partition, QueueState } from './StateQueueStorage';

const STORE = 'queue';
const KEY = 'state';

/** Atomic IndexedDB transactions also fence competing browser tabs. */
export class IndexedDbQueueStorage extends StateQueueStorage {
  private readonly name: string;
  private db?: IDBDatabase;
  private opening?: Promise<void>;
  private channel?: BroadcastChannel;
  private readonly pending = new Set<Promise<unknown>>();
  private readonly listeners = new Set<(target: TargetRef) => void>();

  constructor(name: string) {
    super();
    this.name = name;
  }

  open(): Promise<void> {
    if (this.db) return Promise.resolve();
    if (this.opening) return this.opening;
    if (typeof indexedDB === 'undefined')
      return Promise.reject(new Error('IndexedDB is unavailable'));
    this.opening = new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(this.name, 2);
      request.onupgradeneeded = event => {
        if (!request.result.objectStoreNames.contains(STORE)) {
          request.result.createObjectStore(STORE);
        } else if (request.transaction && event.oldVersion === 1) {
          const store = request.transaction.objectStore(STORE);
          const legacy = store.get(KEY);
          legacy.onsuccess = () => {
            const state = legacy.result as QueueState | undefined;
            if (!state) return;
            for (const [key, part] of Object.entries(state.partitions))
              store.put(part, key);
            store.delete(KEY);
          };
        }
      };
      request.onerror = () =>
        reject(request.error ?? new Error('IndexedDB open failed'));
      request.onblocked = () =>
        reject(new Error('IndexedDB upgrade is blocked'));
      request.onsuccess = () => {
        this.db = request.result;
        this.db.onversionchange = () => {
          this.db?.close();
          this.db = undefined;
        };
        if (typeof BroadcastChannel !== 'undefined') {
          this.channel = new BroadcastChannel(this.name + ':changes');
          this.channel.onmessage = event => this.notify(event.data as string[]);
        }
        resolve();
      };
    }).finally(() => {
      this.opening = undefined;
    });
    return this.opening;
  }

  protected readPart(target: TargetRef): Promise<Partition | undefined> {
    const db = this.db;
    if (!db) return Promise.reject(new Error('queue storage is closed'));
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(STORE, 'readonly');
      const request = transaction.objectStore(STORE).get(targetKey(target));
      let value: Partition | undefined;
      request.onsuccess = () => {
        value = request.result as Partition | undefined;
      };
      transaction.oncomplete = () => resolve(value);
      transaction.onerror = () =>
        reject(transaction.error ?? new Error('IndexedDB read failed'));
      transaction.onabort = () =>
        reject(transaction.error ?? new Error('IndexedDB read aborted'));
    });
  }

  protected updateParts<T>(
    targets: readonly TargetRef[],
    mutate: (parts: Map<string, Partition>) => T
  ): Promise<T> {
    const db = this.db;
    if (!db) return Promise.reject(new Error('queue storage is closed'));
    const keys = [...new Set(targets.map(targetKey))];
    const operation = new Promise<T>((resolve, reject) => {
      const transaction = db.transaction(STORE, 'readwrite');
      const store = transaction.objectStore(STORE);
      const loaded = new Map<string, Partition>();
      let remaining = keys.length;
      let output: T;
      const changed: string[] = [];
      const apply = () => {
        if (--remaining > 0) return;
        try {
          const before = new Map(
            [...loaded].map(([key, part]) => [key, JSON.stringify(part)])
          );
          output = mutate(loaded);
          for (const [key, part] of loaded) {
            if (JSON.stringify(part) !== before.get(key)) {
              changed.push(key);
              store.put(part, key);
            }
          }
        } catch (error) {
          transaction.abort();
          reject(error);
        }
      };
      for (const key of keys) {
        const request = store.get(key);
        request.onsuccess = () => {
          if (request.result) loaded.set(key, request.result as Partition);
          apply();
        };
      }
      transaction.oncomplete = () => {
        if (changed.length) {
          this.channel?.postMessage(changed);
          this.notify(changed);
        }
        resolve(output);
      };
      transaction.onerror = () =>
        reject(transaction.error ?? new Error('IndexedDB write failed'));
      transaction.onabort = () =>
        reject(transaction.error ?? new Error('IndexedDB write aborted'));
    });
    this.pending.add(operation);
    void operation
      .finally(() => this.pending.delete(operation))
      .catch(() => undefined);
    return operation;
  }

  subscribeChanges(notify: (target: TargetRef) => void): () => void {
    this.listeners.add(notify);
    return () => this.listeners.delete(notify);
  }

  private notify(keys: string[]): void {
    for (const key of keys) {
      const [id, partitionKey] = JSON.parse(key) as [string, string];
      for (const listener of this.listeners) {
        try {
          listener({ id, targetKey: partitionKey });
        } catch {
          /* transaction already committed */
        }
      }
    }
  }

  async close(): Promise<void> {
    await Promise.allSettled(this.pending);
    this.channel?.close();
    this.channel = undefined;
    this.db?.close();
    this.db = undefined;
  }
}
