/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'node:assert/strict';
import { createTelemetry } from '../src';
import type { HttpTransport, HttpLogTransformer } from '../src/exporters/http';
import type { LogRecordSnapshot } from '../src/types';

describe('one ConfigSource with two HTTP exporters', () => {
  it('runs the top-level filter once and delivers a batch to both destinations', async () => {
    const received: Record<string, Uint8Array[]> = {
      collector: [],
      external: [],
    };
    const transport = (id: 'collector' | 'external'): HttpTransport => ({
      async send(request) {
        received[id].push(request.body);
        return { status: 200, headers: {}, body: new Uint8Array() };
      },
    });
    const encoder = new TextEncoder();
    const transformer: HttpLogTransformer<{ eventId: string }> = {
      async encode(record: Readonly<LogRecordSnapshot>) {
        return { eventId: record.eventId };
      },
      async buildRequest(items) {
        return {
          body: encoder.encode(JSON.stringify(items)),
          headers: { 'content-type': 'application/json' },
        };
      },
      async parseResponse(_response, items) {
        return {
          kind: 'items',
          items: items.map(item => ({
            eventId: item.eventId,
            status: 'accepted',
          })),
        };
      },
    };
    let filterCalls = 0;
    const telemetry = await createTelemetry({
      exporters: [
        {
          id: 'collector',
          targetKey: 'test:collector',
          type: 'http',
          url: 'https://example.invalid/v1/logs',
          transport: transport('collector'),
        },
        {
          id: 'external',
          targetKey: 'test:external',
          type: 'http',
          url: 'https://example.invalid/events',
          transformer,
          transport: transport('external'),
        },
      ],
      filter: async () => {
        filterCalls++;
        return ['collector', 'external'];
      },
      processor: { type: 'batch', options: { scheduledDelayMillis: 10000 } },
    });
    try {
      const receipt = await telemetry.emit({ eventName: 'game_start' });
      assert.ok(receipt);
      assert.equal(receipt.status, 'submitted');
      assert.equal(filterCalls, 1);
      await telemetry.forceFlush();
      assert.equal(received.collector.length, 1);
      assert.equal(received.external.length, 1);
      assert.match(
        new TextDecoder().decode(received.collector[0]),
        /game_start/
      );
      assert.match(new TextDecoder().decode(received.external[0]), /eventId/);
    } finally {
      await telemetry.shutdown();
    }
  });

  it('makes emit an asynchronous no-op when no exporter is configured', async () => {
    const telemetry = await createTelemetry({
      exporters: [],
      filter: async () => [],
      processor: { type: 'batch' },
    });
    assert.equal(await telemetry.emit({ eventName: 'ignored' }), undefined);
    await telemetry.shutdown();
  });

  it('keeps the Collector working when the external target has no URL', async () => {
    let collectorSends = 0;
    const telemetry = await createTelemetry({
      exporters: [
        {
          id: 'collector',
          targetKey: 'test:collector',
          type: 'http',
          url: 'https://example.invalid/v1/logs',
          transport: {
            async send() {
              collectorSends++;
              return { status: 200, headers: {}, body: new Uint8Array() };
            },
          },
        },
        { id: 'external', targetKey: 'test:external', type: 'http' },
      ],
      filter: async () => ['collector', 'external'],
      processor: { type: 'batch' },
    });
    try {
      const receipt = await telemetry.emit({ eventName: 'game_start' });
      assert.equal(receipt?.status, 'submitted');
      assert.deepEqual(receipt?.unavailable, ['external']);
      await telemetry.forceFlush();
      assert.equal(collectorSends, 1);
    } finally {
      await telemetry.shutdown();
    }
  });

  it('honors the external config source without suppressing Collector', async () => {
    const received: string[] = [];
    const transport = (id: string): HttpTransport => ({
      async send() {
        received.push(id);
        return { status: 200, headers: {}, body: new Uint8Array() };
      },
    });
    const telemetry = await createTelemetry({
      exporters: [
        {
          id: 'collector',
          targetKey: 'test:collector',
          type: 'http',
          url: 'https://example.invalid/v1/logs',
          transport: transport('collector'),
        },
        {
          id: 'external',
          targetKey: 'test:external',
          type: 'http',
          url: 'https://example.invalid/events',
          transport: transport('external'),
          configSources: [
            {
              id: 'remote',
              fallback: { enabled: true },
              initialLoad: 'blocking',
              async load() {
                return { enabled: false };
              },
            },
          ],
        },
      ],
      filter: async () => ['collector', 'external'],
      processor: { type: 'batch' },
    });
    try {
      const receipt = await telemetry.emit({ eventName: 'game_start' });
      assert.equal(receipt?.status, 'submitted');
      assert.deepEqual(receipt?.submitted, ['collector']);
      assert.deepEqual(
        receipt?.suppressed.map(item => item.exporterId),
        ['external']
      );
      await telemetry.forceFlush();
      assert.deepEqual(received, ['collector']);
    } finally {
      await telemetry.shutdown();
    }
  });
});
