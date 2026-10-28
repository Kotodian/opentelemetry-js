/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'assert';
import { IndexedDbQueueStorage } from '../../src/web';
import { targetKey } from '../../src/storage/StateQueueStorage';
import type { QueuedDelivery } from '../../src/storage/QueueStorage';
import type { DeliveryPolicy, TargetRef } from '../../src/types';

const target: TargetRef = { id: 'browser', targetKey: 'browser:test' };
const policy: DeliveryPolicy = {
  triggerCount: 1,
  maxWaitMs: 0,
  maxBatchCount: 10,
  maxBatchBytes: 100000,
  minIntervalMs: 0,
  retryBaseMs: 100,
  retryMaxMs: 1000,
  retryJitterRatio: 0,
  queueCapacity: 10,
  retentionMs: 60000,
  leaseMs: 1000,
};

describe('IndexedDbQueueStorage in a browser', () => {
  it('persists a record and fences concurrent claims', async () => {
    const name = 'durable-logs-browser-' + crypto.randomUUID();
    const first = new IndexedDbQueueStorage(name);
    const second = new IndexedDbQueueStorage(name);
    const delivery: QueuedDelivery = {
      ...target,
      eventId: 'event-1',
      enqueuedAt: 1000,
      snapshot: {
        eventId: 'event-1',
        hrTime: [1, 0],
        hrTimeObserved: [1, 0],
        attributes: { 'event.id': 'event-1' },
        droppedAttributesCount: 0,
        resourceAttributes: {},
        instrumentationScope: { name: 'browser-test' },
      },
    };
    try {
      await Promise.all([first.open(), second.open()]);
      await first.appendFanout(
        [delivery],
        new Map([[targetKey(target), policy]])
      );
      const claims = await Promise.all([
        first.claim(target, policy, 1000, true, new Set()),
        second.claim(target, policy, 1000, true, new Set()),
      ]);
      assert.strictEqual(claims.filter(Boolean).length, 1);
      await Promise.all([first.close(), second.close()]);
      const restored = new IndexedDbQueueStorage(name);
      await restored.open();
      assert.strictEqual(
        (await restored.inspect(target)).head?.eventId,
        'event-1'
      );
      await restored.close();
    } finally {
      await Promise.all([first.close(), second.close()]);
      await new Promise<void>((resolve, reject) => {
        const request = indexedDB.deleteDatabase(name);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
    }
  });
});
