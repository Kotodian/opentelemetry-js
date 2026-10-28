/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'node:assert/strict';
import { context, trace, TraceFlags } from '@opentelemetry/api';
import { LoggerProvider } from '@opentelemetry/sdk-logs';
import {
  DurableDispatchProcessor,
  ExporterRegistry,
  TelemetryEmitter,
} from '../src';
import { MemoryQueueStorage } from '../src/storage/MemoryQueueStorage';
import type {
  AppendResult,
  BatchResult,
  ConfigDecision,
  DeliveryPolicy,
  DurableLogExporter,
  ExporterRegistration,
  LogRecordSnapshot,
  QueuedDelivery,
} from '../src';

const policy: DeliveryPolicy = {
  triggerCount: 100,
  maxWaitMs: 10000,
  maxBatchCount: 10,
  maxBatchBytes: 100000,
  minIntervalMs: 0,
  retryBaseMs: 1000,
  retryMaxMs: 60000,
  retryJitterRatio: 0,
  queueCapacity: 100,
  retentionMs: 86400000,
  leaseMs: 30000,
};

class FakeExporter implements DurableLogExporter {
  records: LogRecordSnapshot[] = [];
  async initialize(): Promise<void> {}
  async export(records: readonly LogRecordSnapshot[]): Promise<BatchResult> {
    this.records.push(...records);
    return {
      kind: 'items',
      items: records.map(record => ({
        eventId: record.eventId,
        status: 'accepted',
      })),
    };
  }
  async shutdown(): Promise<void> {}
}

class FakeStorage extends MemoryQueueStorage {
  fail = false;
  override async appendFanout(
    deliveries: readonly QueuedDelivery[],
    policies: ReadonlyMap<string, DeliveryPolicy>,
    fallbacks?: ReadonlyMap<string, ReadonlyMap<string, ConfigDecision>>
  ): Promise<AppendResult> {
    if (this.fail) throw new Error('storage failed');
    return super.appendFanout(deliveries, policies, fallbacks);
  }
}

function registration(
  id: string,
  filter: ExporterRegistration['filter']
): ExporterRegistration {
  return {
    id,
    targetKey: `test:${id}`,
    filter,
    exporter: new FakeExporter(),
    deliveryPolicy: policy,
  };
}

describe('TelemetryEmitter', () => {
  it('does not call OTel for unmatched or unavailable events', async () => {
    const registry = new ExporterRegistry();
    registry.register(
      registration('one', async record => record.eventName === 'hit')
    );
    const storage = new FakeStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(
      provider.getLogger('test'),
      registry,
      () => 'event-1'
    );
    await processor.start();
    assert.equal(
      (await emitter.emit({ eventName: 'miss' })).status,
      'unmatched'
    );
    assert.equal(
      (await emitter.emit({ eventName: 'hit' })).status,
      'unavailable'
    );
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      0
    );
    await provider.shutdown();
  });

  it('filters before Logger.emit and atomically queues every available target', async () => {
    const registry = new ExporterRegistry();
    const seen: string[] = [];
    registry.register(
      registration('one', async record => {
        seen.push(String(record.attributes['event.id']));
        return true;
      })
    );
    registry.register(registration('two', async () => true));
    await registry.initialize();
    const storage = new FakeStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const logger = provider.getLogger('test');
    const emitter = new TelemetryEmitter(logger, registry, () => 'event-2');
    const receipt = await emitter.emit({
      eventName: 'hit',
      attributes: { score: 7 },
    });
    assert.deepEqual(seen, ['event-2']);
    assert.deepEqual(receipt.queued, ['one', 'two']);
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      1
    );
    assert.equal(
      (await storage.inspect({ id: 'two', targetKey: 'test:two' })).count,
      1
    );
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).head
        ?.snapshot.attributes['score'],
      7
    );
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).head
        ?.snapshot.eventId,
      'event-2'
    );
    logger.emit({ eventName: 'direct' });
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      1
    );
    await provider.shutdown();
  });

  it('rejects when the durable transaction fails', async () => {
    const registry = new ExporterRegistry();
    registry.register(registration('one', async () => true));
    await registry.initialize();
    const storage = new FakeStorage();
    storage.fail = true;
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(
      provider.getLogger('test'),
      registry,
      () => 'event-3'
    );
    await assert.rejects(emitter.emit({ eventName: 'hit' }), /storage failed/);
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      0
    );
    await provider.shutdown();
  });

  it('rejects all admission if one asynchronous filter fails', async () => {
    const registry = new ExporterRegistry();
    registry.register(
      registration('one', async () => {
        throw new Error('filter failed');
      })
    );
    registry.register(registration('two', async () => true));
    await registry.initialize();
    const storage = new FakeStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    await assert.rejects(emitter.emit({ eventName: 'hit' }), /filter failed/);
    assert.equal(
      (await storage.inspect({ id: 'two', targetKey: 'test:two' })).count,
      0
    );
    await provider.shutdown();
  });

  it('times out a stuck filter without entering the processor', async () => {
    const registry = new ExporterRegistry(10);
    registry.register(
      registration('one', async () => new Promise<boolean>(() => undefined))
    );
    await registry.initialize();
    const storage = new FakeStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    await assert.rejects(
      emitter.emit({ eventName: 'hit' }),
      /filter timed out/
    );
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      0
    );
    await provider.shutdown();
  });

  it('dispatches a triggered batch after the durable receipt', async () => {
    const registry = new ExporterRegistry();
    const exporter = new FakeExporter();
    registry.register({
      ...registration('one', async () => true),
      exporter,
      deliveryPolicy: { ...policy, triggerCount: 1 },
    });
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(
      provider.getLogger('test'),
      registry,
      () => 'event-4'
    );
    const receipt = await emitter.emit({ eventName: 'start' });
    assert.equal(receipt.status, 'queued');
    await processor.forceFlush();
    assert.deepEqual(
      exporter.records.map(record => record.eventId),
      ['event-4']
    );
    assert.equal(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).count,
      0
    );
    await provider.shutdown();
  });

  it('loads async policy overrides and reschedules queued work on changes', async () => {
    const batches: string[][] = [];
    const exporter = new (class extends FakeExporter {
      override async export(
        records: readonly LogRecordSnapshot[]
      ): Promise<BatchResult> {
        batches.push(records.map(record => record.eventId));
        return super.export(records);
      }
    })();
    let notify: (() => void) | undefined;
    let weakNetwork = false;
    const registry = new ExporterRegistry();
    registry.register({
      ...registration('one', async () => true),
      exporter,
      deliveryPolicySource: {
        async load() {
          return weakNetwork
            ? { triggerCount: 2, maxBatchCount: 1, maxWaitMs: 1000 }
            : { triggerCount: 100, maxBatchCount: 10, maxWaitMs: 60000 };
        },
        subscribe(callback) {
          notify = callback;
          return () => {
            notify = undefined;
          };
        },
      },
    });
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    let next = 0;
    const emitter = new TelemetryEmitter(
      provider.getLogger('test'),
      registry,
      () => String(++next)
    );
    await emitter.emit({ eventName: 'test' });
    await emitter.emit({ eventName: 'test' });
    assert.equal(batches.length, 0);
    weakNetwork = true;
    notify?.();
    for (let i = 0; i < 50 && batches.length < 2; i++)
      await new Promise(resolve => setTimeout(resolve, 5));
    assert.deepEqual(batches, [['1'], ['2']]);
    await provider.shutdown();
    assert.equal(notify, undefined);
  });

  it('preserves the trace and span context in the durable snapshot', async () => {
    const registry = new ExporterRegistry();
    registry.register(registration('one', async () => true));
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const spanContext = {
      traceId: '11111111111111111111111111111111',
      spanId: '2222222222222222',
      traceFlags: TraceFlags.SAMPLED,
    };
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    await emitter.emit({
      eventName: 'trace',
      context: trace.setSpanContext(context.active(), spanContext),
    });
    assert.deepEqual(
      (await storage.inspect({ id: 'one', targetKey: 'test:one' })).head
        ?.snapshot.spanContext,
      spanContext
    );
    await provider.shutdown();
  });

  it('flushes a queued batch during provider shutdown', async () => {
    const registry = new ExporterRegistry();
    const exporter = new FakeExporter();
    registry.register({
      ...registration('one', async () => true),
      exporter,
    });
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    const receipt = await emitter.emit({ eventName: 'shutdown' });
    await provider.shutdown();
    assert.deepEqual(
      exporter.records.map(record => record.eventId),
      [receipt.eventId]
    );
  });

  it('times out forceFlush while an exporter has not acknowledged', async () => {
    let release!: () => void;
    const acknowledged = new Promise<void>(resolve => {
      release = resolve;
    });
    const exporter = new (class extends FakeExporter {
      override async export(
        records: readonly LogRecordSnapshot[]
      ): Promise<BatchResult> {
        await acknowledged;
        return super.export(records);
      }
    })();
    const registry = new ExporterRegistry();
    registry.register({
      ...registration('one', async () => true),
      exporter,
      deliveryPolicy: { ...policy, triggerCount: 1 },
    });
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    await emitter.emit({ eventName: 'timeout' });
    await assert.rejects(
      processor.forceFlush({ timeoutMillis: 25 }),
      /timed out/
    );
    release();
    await provider.shutdown();
    assert.equal(exporter.records.length, 1);
  });

  it('composes a target configuration source before admitting delivery', async () => {
    const registry = new ExporterRegistry();
    const exporter = new FakeExporter();
    let enabled = false;
    let version = 2;
    let loads = 0;
    let notify: (() => void) | undefined;
    registry.register({
      ...registration('one', async () => true),
      exporter,
      configSources: [
        {
          id: 'test',
          fallback: { enabled: false, version: 2, refreshAfterMs: 60000 },
          initialLoad: 'blocking',
          async load() {
            loads++;
            return { enabled, version, refreshAfterMs: 60000 };
          },
          subscribe(callback) {
            notify = callback;
            return () => {
              notify = undefined;
            };
          },
        },
      ],
    });
    await registry.initialize();
    const storage = new MemoryQueueStorage();
    const processor = new DurableDispatchProcessor({ storage, registry });
    await processor.start();
    const provider = new LoggerProvider({ processors: [processor] });
    const emitter = new TelemetryEmitter(provider.getLogger('test'), registry);
    assert.equal(
      (await emitter.emit({ eventName: 'test' })).status,
      'suppressed'
    );
    enabled = true;
    version = 3;
    notify?.();
    for (
      let i = 0;
      i < 50 &&
      !(await storage.getConfig({ id: 'one', targetKey: 'test:one' }, 'test'))
        ?.decision.enabled;
      i++
    ) {
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    assert.equal(
      (await storage.getConfig({ id: 'one', targetKey: 'test:one' }, 'test'))
        ?.decision.enabled,
      true
    );
    const receipt = await emitter.emit({ eventName: 'test' });
    assert.equal(receipt.status, 'queued');
    await processor.forceFlush();
    assert.equal(exporter.records.length, 1);
    enabled = false;
    version = 2;
    notify?.();
    for (let i = 0; i < 50 && loads < 3; i++) {
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    assert.equal(
      (await storage.getConfig({ id: 'one', targetKey: 'test:one' }, 'test'))
        ?.decision.version,
      3
    );
    await provider.shutdown();
  });
});
