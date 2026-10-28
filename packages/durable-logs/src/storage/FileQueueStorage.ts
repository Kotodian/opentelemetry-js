/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { mkdir, open, rename } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { TargetRef } from '../types';
import { StateQueueStorage, emptyState, targetKey } from './StateQueueStorage';
import type { Partition, QueueState } from './StateQueueStorage';

const MAX_FRAME_BYTES = 64 * 1024 * 1024;
const COMPACT_AT_BYTES = 8 * 1024 * 1024;

function encode(value: unknown, seen = new WeakSet<object>()): unknown {
  if (value === undefined) return ['u'];
  if (value === null || typeof value === 'string' || typeof value === 'boolean')
    return ['p', value];
  if (typeof value === 'number' && Number.isFinite(value)) return ['p', value];
  if (typeof value !== 'object' || seen.has(value))
    throw new TypeError('queue value is not serializable');
  if (value instanceof Uint8Array)
    return ['b', Buffer.from(value).toString('base64')];
  seen.add(value);
  let output: unknown;
  if (Array.isArray(value))
    output = ['a', value.map(item => encode(item, seen))];
  else if (
    Object.getPrototypeOf(value) === Object.prototype ||
    Object.getPrototypeOf(value) === null
  ) {
    output = [
      'o',
      Object.entries(value).map(([key, item]) => [key, encode(item, seen)]),
    ];
  } else throw new TypeError('queue value is not serializable');
  seen.delete(value);
  return output;
}

function decode(value: unknown): unknown {
  if (!Array.isArray(value)) throw new Error('invalid queue journal value');
  switch (value[0]) {
    case 'u':
      return undefined;
    case 'p':
      return value[1];
    case 'b':
      return new Uint8Array(Buffer.from(value[1] as string, 'base64'));
    case 'a':
      return (value[1] as unknown[]).map(decode);
    case 'o':
      return Object.fromEntries(
        (value[1] as Array<[string, unknown]>).map(([key, item]) => [
          key,
          decode(item),
        ])
      );
    default:
      throw new Error('invalid queue journal tag');
  }
}

function checksum(bytes: Uint8Array): number {
  let hash = 2166136261;
  for (const byte of bytes) hash = Math.imul(hash ^ byte, 16777619);
  return hash >>> 0;
}

function frame(partitions: Record<string, Partition>): Buffer {
  const body = Buffer.from(JSON.stringify(encode({ version: 1, partitions })));
  if (body.length > MAX_FRAME_BYTES)
    throw new Error('queue journal frame exceeds maximum size');
  const result = Buffer.allocUnsafe(8 + body.length);
  result.writeUInt32BE(body.length, 0);
  result.writeUInt32BE(checksum(body), 4);
  body.copy(result, 8);
  return result;
}

/** Async, fsync-backed journal for a single desktop main process. */
export class FileQueueStorage extends StateQueueStorage {
  private readonly path: string;
  private handle?: FileHandle;
  private state: QueueState = emptyState();
  private size = 0;
  private gate: Promise<void> = Promise.resolve();
  private readonly listeners = new Set<(target: TargetRef) => void>();

  constructor(path: string) {
    super();
    this.path = path;
  }

  async open(): Promise<void> {
    if (this.handle) return;
    await mkdir(dirname(this.path), { recursive: true });
    const handle = await open(this.path, 'a+');
    try {
      const bytes = await handle.readFile();
      let offset = 0;
      const restored = emptyState();
      while (offset + 8 <= bytes.length) {
        const length = bytes.readUInt32BE(offset);
        if (length > MAX_FRAME_BYTES || offset + 8 + length > bytes.length)
          break;
        const payload = bytes.subarray(offset + 8, offset + 8 + length);
        if (bytes.readUInt32BE(offset + 4) !== checksum(payload)) break;
        let decoded: QueueState;
        try {
          decoded = decode(JSON.parse(payload.toString())) as QueueState;
        } catch {
          break;
        }
        if (decoded.version !== 1 || !decoded.partitions)
          throw new Error('unsupported queue journal schema');
        Object.assign(restored.partitions, decoded.partitions);
        offset += 8 + length;
      }
      if (offset < bytes.length) {
        await handle.truncate(offset);
        await handle.sync();
      }
      this.state = restored;
      this.size = offset;
      this.handle = handle;
    } catch (error) {
      await handle.close();
      throw error;
    }
  }

  protected async readPart(target: TargetRef): Promise<Partition | undefined> {
    if (!this.handle) throw new Error('queue storage is closed');
    await this.gate;
    const part = this.state.partitions[targetKey(target)];
    return part ? structuredClone(part) : undefined;
  }

  protected updateParts<T>(
    targets: readonly TargetRef[],
    mutate: (parts: Map<string, Partition>) => T
  ): Promise<T> {
    if (!this.handle)
      return Promise.reject(new Error('queue storage is closed'));
    const result = this.gate.then(async () => {
      const parts = new Map<string, Partition>();
      for (const target of targets) {
        const key = targetKey(target);
        const part = this.state.partitions[key];
        if (part) parts.set(key, structuredClone(part));
      }
      const output = mutate(parts);
      const changed: Record<string, Partition> = {};
      for (const [key, value] of parts) {
        if (
          JSON.stringify(value) !== JSON.stringify(this.state.partitions[key])
        )
          changed[key] = value;
      }
      if (Object.keys(changed).length) {
        const bytes = frame(changed);
        const handle = this.handle;
        if (!handle) throw new Error('queue storage is closed');
        try {
          await handle.writeFile(bytes);
          await handle.sync();
        } catch (error) {
          await handle.truncate(this.size);
          await handle.sync();
          throw error;
        }
        this.size += bytes.length;
        Object.assign(this.state.partitions, changed);
        for (const key of Object.keys(changed)) {
          const [id, partitionKey] = JSON.parse(key) as [string, string];
          for (const listener of this.listeners) {
            try {
              listener({ id, targetKey: partitionKey });
            } catch {
              /* committed transaction remains successful */
            }
          }
        }
        if (this.size >= COMPACT_AT_BYTES) {
          // Compaction is maintenance; a committed append must still return success.
          try {
            await this.compact();
          } catch {
            /* replayable journal remains authoritative */
          }
        }
      }
      return output;
    });
    this.gate = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  private async compact(): Promise<void> {
    const tempPath = this.path + '.new';
    const temp = await open(tempPath, 'w');
    const bytes = frame(this.state.partitions);
    try {
      await temp.writeFile(bytes);
      await temp.sync();
    } finally {
      await temp.close();
    }
    const replacement = await open(tempPath, 'a+');
    try {
      await rename(tempPath, this.path);
    } catch (error) {
      await replacement.close();
      throw error;
    }
    const old = this.handle;
    this.handle = replacement;
    this.size = bytes.length;
    await old?.close().catch(() => undefined);
  }

  subscribeChanges(notify: (target: TargetRef) => void): () => void {
    this.listeners.add(notify);
    return () => this.listeners.delete(notify);
  }

  async close(): Promise<void> {
    await this.gate;
    await this.handle?.close();
    this.handle = undefined;
  }
}
