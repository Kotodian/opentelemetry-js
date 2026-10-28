/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as assert from 'node:assert/strict';
import type { HttpRequest, HttpTransport } from '../src/exporters/http';
import {
  HttpLogExporter,
  HttpRequestTooLargeError,
} from '../src/exporters/http';
import type { LogRecordSnapshot } from '../src/types';

const snapshot: LogRecordSnapshot = {
  eventId: 'event-1',
  eventName: 'purchase',
  hrTime: [1, 0],
  hrTimeObserved: [1, 0],
  spanContext: {
    traceId: '11111111111111111111111111111111',
    spanId: '2222222222222222',
    traceFlags: 1,
  },
  attributes: { 'event.id': 'event-1', 'event.name': 'purchase' },
  body: { count: 1 },
  droppedAttributesCount: 0,
  resourceAttributes: { 'service.name': 'test' },
  instrumentationScope: { name: 'test' },
};

describe('HttpLogExporter', () => {
  it('uses official OTLP JSON when a mock transport is supplied without a transformer', async () => {
    let request: HttpRequest | undefined;
    const transport: HttpTransport = {
      async send(value) {
        request = value;
        return { status: 200, headers: {}, body: new Uint8Array() };
      },
    };
    const exporter = new HttpLogExporter({
      url: 'https://example.test/v1/logs',
      headers: { Authorization: 'test' },
      transport,
    });
    await exporter.initialize();
    assert.deepEqual(await exporter.export([snapshot]), {
      kind: 'items',
      items: [{ eventId: 'event-1', status: 'accepted' }],
    });
    assert.equal(request?.headers['Content-Type'], 'application/json');
    assert.equal(request?.headers.Authorization, 'test');
    assert.equal(request?.url, 'https://example.test/v1/logs');
    const body = JSON.parse(new TextDecoder().decode(request?.body));
    assert.equal(
      body.resourceLogs[0].scopeLogs[0].logRecords[0].eventName,
      'purchase'
    );
    assert.equal(
      body.resourceLogs[0].scopeLogs[0].logRecords[0].traceId,
      snapshot.spanContext?.traceId
    );
    assert.equal(
      body.resourceLogs[0].scopeLogs[0].logRecords[0].spanId,
      snapshot.spanContext?.spanId
    );
    await exporter.shutdown();
  });

  it('sends transformer bytes unchanged and preserves a permanent local rejection', async () => {
    const requests: HttpRequest[] = [];
    const transport: HttpTransport = {
      async send(request) {
        requests.push(request);
        return {
          status: 207,
          headers: {},
          body: new TextEncoder().encode('accepted'),
        };
      },
    };
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      headers: { Authorization: 'test' },
      transport,
      transformer: {
        async encode(record) {
          return record.eventId === 'event-2'
            ? {
                kind: 'reject' as const,
                eventId: record.eventId,
                reason: 'invalid',
              }
            : { eventId: record.eventId };
        },
        async buildRequest(items) {
          return {
            body: new TextEncoder().encode(
              items.map(item => item.eventId).join(',')
            ),
            headers: { 'X-Test': 'yes' },
          };
        },
        async parseResponse(response, items) {
          assert.equal(new TextDecoder().decode(response.body), 'accepted');
          return {
            kind: 'items' as const,
            items: items.map(item => ({
              eventId: item.eventId,
              status: 'accepted' as const,
            })),
          };
        },
      },
    });
    const result = await exporter.export([
      snapshot,
      { ...snapshot, eventId: 'event-2' },
    ]);
    assert.deepEqual(result, {
      kind: 'items',
      items: [
        { eventId: 'event-2', status: 'rejected' },
        { eventId: 'event-1', status: 'accepted' },
      ],
    });
    assert.equal(new TextDecoder().decode(requests[0].body), 'event-1');
    assert.equal(requests[0].headers['X-Test'], 'yes');
    assert.equal(requests[0].headers.Authorization, 'test');
  });

  it('keeps the batch retryable when the transformer response omits an event', async () => {
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      transport: {
        async send() {
          return { status: 200, headers: {}, body: new Uint8Array() };
        },
      },
      transformer: {
        async encode(record) {
          return { eventId: record.eventId };
        },
        async buildRequest() {
          return { body: new Uint8Array(), headers: {} };
        },
        async parseResponse() {
          return { kind: 'items' as const, items: [] };
        },
      },
    });
    assert.deepEqual(await exporter.export([snapshot]), { kind: 'retry' });
  });

  it('splits an oversized encoded batch and rejects only an oversized single item', async () => {
    const sent: string[] = [];
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      maxRequestBytes: 4,
      transport: {
        async send(request) {
          sent.push(new TextDecoder().decode(request.body));
          return { status: 200, headers: {}, body: new Uint8Array() };
        },
      },
      transformer: {
        async encode(record) {
          return { eventId: record.eventId };
        },
        async buildRequest(items) {
          return {
            body: new TextEncoder().encode(
              items.map(item => item.eventId).join(',')
            ),
            headers: {},
          };
        },
        async parseResponse(_response, items) {
          return {
            kind: 'items' as const,
            items: items.map(item => ({
              eventId: item.eventId,
              status: 'accepted' as const,
            })),
          };
        },
      },
    });
    const result = await exporter.export(
      ['a', 'too-large', 'b'].map(eventId => ({ ...snapshot, eventId }))
    );
    assert.deepEqual(result, {
      kind: 'items',
      items: [
        { eventId: 'a', status: 'accepted' },
        { eventId: 'too-large', status: 'rejected' },
        { eventId: 'b', status: 'accepted' },
      ],
    });
    assert.deepEqual(sent, ['a', 'b']);
  });

  it('splits HTTP 413 and transformer size errors without retrying the whole batch', async () => {
    const sent: string[] = [];
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      transport: {
        async send(request) {
          const value = new TextDecoder().decode(request.body);
          sent.push(value);
          return {
            status: value.includes('large') ? 413 : 200,
            headers: {},
            body: new Uint8Array(),
          };
        },
      },
      transformer: {
        async encode(record) {
          return { eventId: record.eventId };
        },
        async buildRequest(items) {
          if (items.length > 2) throw new HttpRequestTooLargeError();
          return {
            body: new TextEncoder().encode(
              items.map(item => item.eventId).join(',')
            ),
            headers: {},
          };
        },
        async parseResponse(_response, items) {
          return {
            kind: 'items' as const,
            items: items.map(item => ({
              eventId: item.eventId,
              status: 'accepted' as const,
            })),
          };
        },
      },
    });
    const result = await exporter.export(
      ['a', 'large', 'b'].map(eventId => ({ ...snapshot, eventId }))
    );
    assert.deepEqual(result, {
      kind: 'items',
      items: [
        { eventId: 'a', status: 'accepted' },
        { eventId: 'large', status: 'rejected' },
        { eventId: 'b', status: 'accepted' },
      ],
    });
    assert.deepEqual(sent, ['a', 'large,b', 'large', 'b']);
  });

  it('preserves an exact retry-after delay for a partially limited custom batch', async () => {
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      transport: {
        async send() {
          return { status: 429, headers: {}, body: new Uint8Array() };
        },
      },
      transformer: {
        async encode(record) {
          return record.eventId === 'rejected'
            ? {
                kind: 'reject' as const,
                eventId: record.eventId,
                reason: 'bad',
              }
            : { eventId: record.eventId };
        },
        async buildRequest() {
          return { body: new Uint8Array(), headers: {} };
        },
        async parseResponse() {
          return { kind: 'pause' as const, retryAfterMs: 250 };
        },
      },
    });
    assert.deepEqual(
      await exporter.export([
        { ...snapshot, eventId: 'rejected' },
        { ...snapshot, eventId: 'retry' },
      ]),
      {
        kind: 'items',
        items: [
          { eventId: 'rejected', status: 'rejected' },
          { eventId: 'retry', status: 'retryable' },
        ],
        retryAfterMs: 250,
        retryAfterExact: true,
      }
    );
  });

  it('spaces split HTTP requests using an asynchronous caller policy', async () => {
    const sentAt: number[] = [];
    let policyLoads = 0;
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      maxRequestBytes: 3,
      requestIntervalMs: async () => {
        policyLoads++;
        return 20;
      },
      transport: {
        async send() {
          sentAt.push(Date.now());
          return { status: 200, headers: {}, body: new Uint8Array() };
        },
      },
      transformer: {
        async encode(record) {
          return { eventId: record.eventId };
        },
        async buildRequest(items) {
          return {
            body: new TextEncoder().encode(
              items.map(item => item.eventId).join(',')
            ),
            headers: {},
          };
        },
        async parseResponse(_response, items) {
          return {
            kind: 'items' as const,
            items: items.map(item => ({
              eventId: item.eventId,
              status: 'accepted' as const,
            })),
          };
        },
      },
    });
    const result = await exporter.export(
      ['aa', 'bb'].map(eventId => ({ ...snapshot, eventId }))
    );
    assert.equal(result.kind, 'items');
    assert.equal(policyLoads, 2);
    assert.equal(sentAt.length, 2);
    assert.ok(sentAt[1] - sentAt[0] >= 15);
  });

  it('does not send later split chunks after a server-directed pause', async () => {
    const sent: string[] = [];
    const exporter = new HttpLogExporter({
      url: 'https://example.test/events',
      maxRequestBytes: 1,
      transport: {
        async send(request) {
          sent.push(new TextDecoder().decode(request.body));
          return { status: 429, headers: {}, body: new Uint8Array() };
        },
      },
      transformer: {
        async encode(record) {
          return { eventId: record.eventId };
        },
        async buildRequest(items) {
          return {
            body: new TextEncoder().encode(
              items.map(item => item.eventId).join(',')
            ),
            headers: {},
          };
        },
        async parseResponse() {
          return { kind: 'pause' as const, retryAfterMs: 250 };
        },
      },
    });
    assert.deepEqual(
      await exporter.export(
        ['a', 'b'].map(eventId => ({ ...snapshot, eventId }))
      ),
      {
        kind: 'items',
        items: [
          { eventId: 'a', status: 'retryable' },
          { eventId: 'b', status: 'retryable' },
        ],
        retryAfterMs: 250,
        retryAfterExact: true,
      }
    );
    assert.deepEqual(sent, ['a']);
  });
});
