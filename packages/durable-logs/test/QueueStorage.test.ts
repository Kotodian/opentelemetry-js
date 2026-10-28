/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'node:assert/strict';
import 'fake-indexeddb/auto';
import { appendFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FileQueueStorage } from '../src/storage/FileQueueStorage';
import { IndexedDbQueueStorage } from '../src/storage/IndexedDbQueueStorage';
import { MemoryQueueStorage } from '../src/storage/MemoryQueueStorage';
import { targetKey } from '../src/storage/StateQueueStorage';
import type {
  DeliveryPolicy,
  LogRecordSnapshot,
  TargetRef,
} from '../src/types';
import type { QueuedDelivery } from '../src/storage/QueueStorage';

const target: TargetRef = { id: 'collector', targetKey: 'test:collector' };
const second: TargetRef = { id: 'other', targetKey: 'test:other' };
const policy: DeliveryPolicy = {
  triggerCount: 2,
  maxWaitMs: 1000,
  maxBatchCount: 10,
  maxBatchBytes: 100000,
  minIntervalMs: 0,
  retryBaseMs: 100,
  retryMaxMs: 1000,
  retryJitterRatio: 0,
  queueCapacity: 2,
  retentionMs: 86400000,
  leaseMs: 1000,
};

function delivery(ref: TargetRef, eventId: string): QueuedDelivery {
  const snapshot: LogRecordSnapshot = {
    eventId,
    eventName: 'test',
    hrTime: [1, 0],
    hrTimeObserved: [1, 0],
    attributes: { 'event.id': eventId },
    droppedAttributesCount: 0,
    resourceAttributes: {},
    instrumentationScope: { name: 'test' },
  };
  return { ...ref, eventId, snapshot, enqueuedAt: 1000 };
}

const policies = new Map([
  [targetKey(target), policy],
  [targetKey(second), policy],
]);

describe('durable queue rules', () => {
  it('atomically rejects a fan-out when one target is full', async () => {
    const storage = new MemoryQueueStorage();
    await storage.open();
    await storage.appendFanout(
      [delivery(target, 'one'), delivery(target, 'two')],
      policies
    );
    await assert.rejects(
      storage.appendFanout(
        [delivery(second, 'three'), delivery(target, 'three')],
        policies
      ),
      /capacity/
    );
    assert.equal((await storage.inspect(second)).count, 0);
    assert.equal((await storage.inspect(target)).count, 2);
    await storage.close();
  });

  it('rejects unsupported snapshots before any target is written', async () => {
    const storage = new MemoryQueueStorage();
    await storage.open();
    const invalid = delivery(target, 'invalid');
    await assert.rejects(
      storage.appendFanout(
        [
          delivery(second, 'valid'),
          { ...invalid, snapshot: { ...invalid.snapshot, body: new Date() } },
        ],
        policies
      ),
      /not serializable/
    );
    assert.equal((await storage.inspect(second)).count, 0);
    await storage.close();
  });

  it('rechecks the latest persisted configuration in the append transaction', async () => {
    const storage = new MemoryQueueStorage();
    await storage.open();
    await storage.setConfig(target, 'remote', {
      decision: { enabled: false },
      refreshAt: 9000,
    });
    const fallbacks = new Map([
      [targetKey(target), new Map([['remote', { enabled: true }]])],
    ]);
    const result = await storage.appendFanout(
      [delivery(target, 'one'), delivery(second, 'one')],
      policies,
      fallbacks
    );
    assert.deepEqual(
      result.suppressed.map(item => item.id),
      ['collector']
    );
    assert.deepEqual(
      result.queued.map(item => item.id),
      ['other']
    );
    assert.equal((await storage.inspect(target)).count, 0);
    assert.equal((await storage.inspect(second)).count, 1);
    await storage.close();
  });

  it('rechecks persisted configuration before leasing a batch', async () => {
    const storage = new MemoryQueueStorage();
    await storage.open();
    await storage.appendFanout([delivery(target, 'one')], policies);
    const fallback = new Map([['remote', { enabled: true }]]);
    await storage.setConfig(target, 'remote', {
      decision: { enabled: false },
      refreshAt: 9000,
    });
    assert.equal(
      await storage.claim(target, policy, 1000, true, new Set(), fallback),
      undefined
    );
    assert.equal((await storage.inspect(target)).count, 1);
    await storage.setConfig(target, 'remote', {
      decision: { enabled: true },
      refreshAt: 10000,
    });
    assert.ok(
      await storage.claim(target, policy, 1000, true, new Set(), fallback)
    );
    await storage.close();
  });

  it('keeps a failed item and fences an expired lease', async () => {
    const storage = new MemoryQueueStorage();
    await storage.open();
    await storage.appendFanout([delivery(target, 'one')], policies);
    const first = await storage.claim(target, policy, 1000, true, new Set());
    assert.ok(first);
    assert.equal(
      await storage.settle(
        target,
        first.token,
        { kind: 'retry', retryAfterMs: Number.NaN },
        policy,
        1100
      ),
      true
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 1200);
    assert.equal(
      await storage.claim(target, policy, 1199, true, new Set()),
      undefined
    );
    const secondClaim = await storage.claim(
      target,
      policy,
      1200,
      true,
      new Set()
    );
    assert.ok(secondClaim);
    assert.equal(
      await storage.settle(
        target,
        first.token,
        { kind: 'items', items: [{ eventId: 'one', status: 'accepted' }] },
        policy,
        1201
      ),
      false
    );
    assert.equal(
      await storage.settle(
        target,
        secondClaim.token,
        { kind: 'items', items: [{ eventId: 'one', status: 'accepted' }] },
        policy,
        1201
      ),
      true
    );
    assert.equal((await storage.inspect(target)).count, 0);
    await storage.close();
  });

  it('persists a per-target retry ladder across file storage restarts', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'otel-retry-ladder-'));
    const path = join(directory, 'queue.journal');
    const ladder = {
      ...policy,
      retryDelaysMs: [2000, 30000, 300000, 1800000],
    };
    let storage = new FileQueueStorage(path);
    try {
      await storage.open();
      await storage.appendFanout(
        [delivery(target, 'one')],
        new Map([[targetKey(target), ladder]])
      );
      let now = 1000;
      for (const delay of ladder.retryDelaysMs) {
        const claim = await storage.claim(target, ladder, now, true, new Set());
        assert.ok(claim);
        await storage.settle(
          target,
          claim.token,
          { kind: 'retry' },
          ladder,
          now + 1
        );
        assert.equal(
          (await storage.inspect(target)).head?.retryAt,
          now + 1 + delay
        );
        now += delay + 1;
        await storage.close();
        storage = new FileQueueStorage(path);
        await storage.open();
      }
      const last = await storage.claim(target, ladder, now, true, new Set());
      assert.ok(last);
      await storage.settle(
        target,
        last.token,
        { kind: 'retry' },
        ladder,
        now + 1
      );
      assert.equal(
        (await storage.inspect(target)).head?.retryAt,
        now + 1800001
      );
    } finally {
      await storage.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it('uses exact rate-limit delays without advancing the failure ladder', async () => {
    const storage = new MemoryQueueStorage();
    const ladder = { ...policy, retryDelaysMs: [2000, 30000] };
    await storage.open();
    await storage.appendFanout(
      [delivery(target, 'one')],
      new Map([[targetKey(target), ladder]])
    );
    const first = await storage.claim(target, ladder, 1000, true, new Set());
    assert.ok(first);
    await storage.settle(
      target,
      first.token,
      { kind: 'pause', retryAfterMs: 500 },
      ladder,
      1001
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 1501);
    const secondClaim = await storage.claim(
      target,
      ladder,
      1501,
      true,
      new Set()
    );
    assert.ok(secondClaim);
    await storage.settle(
      target,
      secondClaim.token,
      { kind: 'retry' },
      ladder,
      1502
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 3502);
    const third = await storage.claim(target, ladder, 3502, true, new Set());
    assert.ok(third);
    await storage.settle(
      target,
      third.token,
      { kind: 'pause', retryAfterMs: Number.NaN },
      ladder,
      3503
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 33503);
    await storage.close();
  });

  it('keeps the failure step for partial acknowledgements and exact item delays', async () => {
    const storage = new MemoryQueueStorage();
    const ladder = {
      ...policy,
      queueCapacity: 4,
      retryDelaysMs: [2000, 30000],
    };
    const policies = new Map([[targetKey(target), ladder]]);
    await storage.open();
    await storage.appendFanout(
      [delivery(target, 'one'), delivery(target, 'two')],
      policies
    );
    let claim = await storage.claim(target, ladder, 1000, true, new Set());
    assert.ok(claim);
    await storage.settle(
      target,
      claim.token,
      {
        kind: 'items',
        items: [
          { eventId: 'one', status: 'accepted' },
          { eventId: 'two', status: 'retryable' },
        ],
      },
      ladder,
      1001
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 3001);
    claim = await storage.claim(target, ladder, 3001, true, new Set());
    assert.ok(claim);
    await storage.settle(
      target,
      claim.token,
      {
        kind: 'items',
        items: [{ eventId: 'two', status: 'retryable' }],
        retryAfterMs: 500,
        retryAfterExact: true,
      },
      ladder,
      3002
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 3502);
    claim = await storage.claim(target, ladder, 3502, true, new Set());
    assert.ok(claim);
    await storage.settle(target, claim.token, { kind: 'retry' }, ladder, 3503);
    assert.equal((await storage.inspect(target)).head?.retryAt, 33503);
    await storage.close();
  });

  it('does not trust a rate-limit hint attached to a malformed item acknowledgement', async () => {
    const storage = new MemoryQueueStorage();
    const ladder = { ...policy, retryDelaysMs: [2000, 30000] };
    await storage.open();
    await storage.appendFanout(
      [delivery(target, 'one')],
      new Map([[targetKey(target), ladder]])
    );
    const claim = await storage.claim(target, ladder, 1000, true, new Set());
    assert.ok(claim);
    await storage.settle(
      target,
      claim.token,
      {
        kind: 'items',
        items: [{ eventId: 'unknown', status: 'accepted' }],
        retryAfterMs: 1,
        retryAfterExact: true,
      },
      ladder,
      1001
    );
    assert.equal((await storage.inspect(target)).head?.retryAt, 3001);
    await storage.close();
  });

  it('supports symmetric jitter and resets only after an accepted batch', async () => {
    const storage = new MemoryQueueStorage();
    const jittered = {
      ...policy,
      queueCapacity: 4,
      retryDelaysMs: [2000, 30000],
      retryJitterMode: 'symmetric' as const,
      retryJitterRatio: 0.3,
    };
    const policies = new Map([[targetKey(target), jittered]]);
    const random = Math.random;
    await storage.open();
    try {
      await storage.appendFanout([delivery(target, 'one')], policies);
      let claim = await storage.claim(target, jittered, 1000, true, new Set());
      assert.ok(claim);
      Math.random = () => 0;
      await storage.settle(
        target,
        claim.token,
        { kind: 'retry' },
        jittered,
        1001
      );
      assert.equal((await storage.inspect(target)).head?.retryAt, 2401);
      claim = await storage.claim(target, jittered, 2401, true, new Set());
      assert.ok(claim);
      Math.random = () => 1;
      await storage.settle(
        target,
        claim.token,
        { kind: 'retry' },
        jittered,
        2402
      );
      assert.equal((await storage.inspect(target)).head?.retryAt, 41402);
      claim = await storage.claim(target, jittered, 41402, true, new Set());
      assert.ok(claim);
      await storage.settle(
        target,
        claim.token,
        { kind: 'items', items: [{ eventId: 'one', status: 'rejected' }] },
        jittered,
        41403
      );
      await storage.appendFanout([delivery(target, 'two')], policies);
      claim = await storage.claim(target, jittered, 41403, true, new Set());
      assert.ok(claim);
      Math.random = () => 0.5;
      await storage.settle(
        target,
        claim.token,
        { kind: 'retry' },
        jittered,
        41404
      );
      assert.equal((await storage.inspect(target)).head?.retryAt, 71404);
      claim = await storage.claim(target, jittered, 71404, true, new Set());
      assert.ok(claim);
      await storage.settle(
        target,
        claim.token,
        { kind: 'items', items: [{ eventId: 'two', status: 'accepted' }] },
        jittered,
        71405
      );
      await storage.appendFanout([delivery(target, 'three')], policies);
      claim = await storage.claim(target, jittered, 71405, true, new Set());
      assert.ok(claim);
      await storage.settle(
        target,
        claim.token,
        { kind: 'retry' },
        jittered,
        71406
      );
      assert.equal((await storage.inspect(target)).head?.retryAt, 73406);
    } finally {
      Math.random = random;
      await storage.close();
    }
  });

  it('evicts the oldest unleased items at a configured high watermark', async () => {
    const storage = new MemoryQueueStorage();
    const evicting = {
      ...policy,
      queueCapacity: 10,
      maxBatchCount: 2,
      queueEviction: { highWatermark: 9, fraction: 0.5 },
    };
    const policies = new Map([[targetKey(target), evicting]]);
    await storage.open();
    await storage.appendFanout(
      Array.from({ length: 8 }, (_, i) => delivery(target, String(i + 1))),
      policies
    );
    const leased = await storage.claim(target, evicting, 1000, true, new Set());
    assert.deepEqual(
      leased?.entries.map(item => item.eventId),
      ['1', '2']
    );
    await storage.appendFanout([delivery(target, '9')], policies);
    assert.equal((await storage.inspect(target)).count, 5);
    assert.equal((await storage.inspect(target)).head?.eventId, '1');
    assert.ok(leased);
    await storage.settle(
      target,
      leased.token,
      {
        kind: 'items',
        items: [
          { eventId: '1', status: 'accepted' },
          { eventId: '2', status: 'accepted' },
        ],
      },
      evicting,
      1001
    );
    const remaining = await storage.claim(
      target,
      evicting,
      1001,
      true,
      new Set()
    );
    assert.deepEqual(
      remaining?.entries.map(item => item.eventId),
      ['7', '8']
    );
    await storage.close();
  });

  it('replays complete desktop journal frames and discards a partial tail', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'otel-durable-queue-'));
    const path = join(directory, 'queue.journal');
    try {
      const first = new FileQueueStorage(path);
      await first.open();
      await first.appendFanout(
        [
          {
            ...delivery(target, 'one'),
            snapshot: {
              ...delivery(target, 'one').snapshot,
              body: new Uint8Array([0, 255]),
            },
          },
          delivery(second, 'one'),
        ],
        policies
      );
      await first.setConfig(target, 'remote', {
        decision: { enabled: false },
        refreshAt: 9000,
      });
      await first.close();
      await appendFile(path, Buffer.from([0, 1, 2]));
      const restored = new FileQueueStorage(path);
      await restored.open();
      assert.equal((await restored.inspect(target)).head?.eventId, 'one');
      assert.deepEqual(
        (await restored.inspect(target)).head?.snapshot.body,
        new Uint8Array([0, 255])
      );
      assert.equal((await restored.inspect(second)).count, 1);
      assert.equal(
        (await restored.getConfig(target, 'remote'))?.decision.enabled,
        false
      );
      await restored.close();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it('serializes browser tabs and restores their common queue', async () => {
    const name = 'otel-queue-test-' + crypto.randomUUID();
    const first = new IndexedDbQueueStorage(name);
    const secondTab = new IndexedDbQueueStorage(name);
    await Promise.all([first.open(), secondTab.open()]);
    await first.appendFanout([delivery(target, 'one')], policies);
    const claims = await Promise.all([
      first.claim(target, policy, 1000, true, new Set()),
      secondTab.claim(target, policy, 1000, true, new Set()),
    ]);
    assert.equal(claims.filter(Boolean).length, 1);
    await Promise.all([first.close(), secondTab.close()]);
    const restored = new IndexedDbQueueStorage(name);
    await restored.open();
    assert.equal((await restored.inspect(target)).head?.eventId, 'one');
    await restored.close();
    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.deleteDatabase(name);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  });
});
