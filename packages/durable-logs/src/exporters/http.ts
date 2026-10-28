/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { JsonLogsSerializer } from '@opentelemetry/otlp-transformer';
import { ExportResultCode } from '@opentelemetry/core';
import { resourceFromAttributes } from '@opentelemetry/resources';
import type {
  BatchResult,
  DurableLogExporter,
  LogRecordSnapshot,
} from '../types';

export interface EncodedHttpItem {
  eventId: string;
}
export interface PermanentReject {
  kind: 'reject';
  eventId: string;
  reason: string;
}
/** A transformer may throw this to let the exporter split an oversized batch. */
export class HttpRequestTooLargeError extends Error {
  constructor(message = 'HTTP request exceeds the destination limit') {
    super(message);
    this.name = 'HttpRequestTooLargeError';
  }
}
export interface HttpResponse {
  status: number;
  headers: Readonly<Record<string, string>>;
  body: Uint8Array;
}
export interface HttpRequest {
  url: string;
  body: Uint8Array;
  headers: Readonly<Record<string, string>>;
  timeoutMs: number;
}
export interface HttpTransport {
  send(request: HttpRequest): Promise<HttpResponse>;
}
export interface HttpLogTransformer<Item extends EncodedHttpItem> {
  encode(record: Readonly<LogRecordSnapshot>): Promise<Item | PermanentReject>;
  buildRequest(
    items: readonly Item[]
  ): Promise<{ body: Uint8Array; headers: Readonly<Record<string, string>> }>;
  parseResponse(
    response: HttpResponse,
    items: readonly Item[]
  ): Promise<BatchResult>;
}
export interface HttpLogExporterOptions<Item extends EncodedHttpItem> {
  url: string;
  headers?: Record<string, string>;
  transformer?: HttpLogTransformer<Item>;
  transport?: HttpTransport;
  timeoutMs?: number;
  maxResponseBytes?: number;
  /** Optional encoded request limit; an oversized single item is rejected. */
  maxRequestBytes?: number;
  /** Async per-request spacing, including requests made when a batch splits. */
  requestIntervalMs?: number | (() => Promise<number>);
}

function readable(record: LogRecordSnapshot): ReadableLogRecord {
  return {
    hrTime: record.hrTime,
    hrTimeObserved: record.hrTimeObserved,
    spanContext: record.spanContext,
    attributes: record.attributes,
    body: record.body,
    eventName: record.eventName,
    severityNumber: record.severityNumber,
    severityText: record.severityText,
    droppedAttributesCount: record.droppedAttributesCount,
    resource: resourceFromAttributes(record.resourceAttributes, {
      schemaUrl: record.resourceSchemaUrl,
    }),
    instrumentationScope: record.instrumentationScope,
  };
}

async function readLimited(
  response: Response,
  maxBytes: number
): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const parts: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      length += next.value.byteLength;
      if (length > maxBytes)
        throw new Error('HTTP response body exceeds maxResponseBytes');
      parts.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
}

function fetchTransport(maxResponseBytes: number): HttpTransport {
  return {
    async send(request): Promise<HttpResponse> {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), request.timeoutMs);
      try {
        const response = await fetch(request.url, {
          method: 'POST',
          headers: request.headers,
          body: request.body,
          signal: controller.signal,
        });
        const headers: Record<string, string> = {};
        response.headers.forEach((value, key) => {
          headers[key] = value;
        });
        return {
          status: response.status,
          headers,
          body: await readLimited(response, maxResponseBytes),
        };
      } finally {
        clearTimeout(timeout);
      }
    },
  };
}

function retryAfterMs(
  headers: Readonly<Record<string, string>>
): number | undefined {
  const value = headers['retry-after'] ?? headers['Retry-After'];
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : undefined;
}

/** One HTTP exporter type; only the optional transformer changes wire format. */
export class HttpLogExporter<Item extends EncodedHttpItem = EncodedHttpItem>
  implements DurableLogExporter
{
  private readonly native?: OTLPLogExporter;
  private readonly transport: HttpTransport;
  private readonly timeoutMs: number;
  private readonly options: HttpLogExporterOptions<Item>;
  private sendTail: Promise<void> = Promise.resolve();
  private lastRequestStartedAt = 0;
  private stopped = false;

  constructor(options: HttpLogExporterOptions<Item>) {
    this.options = options;
    if (!options.url) throw new TypeError('url is required');
    this.timeoutMs = options.timeoutMs ?? 30000;
    if (!Number.isFinite(this.timeoutMs) || this.timeoutMs <= 0)
      throw new TypeError('timeoutMs must be positive');
    if (
      options.maxResponseBytes !== undefined &&
      (!Number.isSafeInteger(options.maxResponseBytes) ||
        options.maxResponseBytes <= 0)
    )
      throw new TypeError('maxResponseBytes must be positive');
    if (
      options.maxRequestBytes !== undefined &&
      (!Number.isSafeInteger(options.maxRequestBytes) ||
        options.maxRequestBytes <= 0)
    )
      throw new TypeError('maxRequestBytes must be positive');
    if (
      typeof options.requestIntervalMs === 'number' &&
      (!Number.isFinite(options.requestIntervalMs) ||
        options.requestIntervalMs < 0)
    )
      throw new TypeError('requestIntervalMs must be non-negative');
    this.transport =
      options.transport ??
      fetchTransport(options.maxResponseBytes ?? 1024 * 1024);
    if (!options.transformer && !options.transport)
      this.native = new OTLPLogExporter({
        url: options.url,
        headers: options.headers,
        timeoutMillis: this.timeoutMs,
      });
  }

  async initialize(): Promise<void> {
    if (this.stopped) throw new Error('exporter is shut down');
  }

  async export(records: readonly LogRecordSnapshot[]): Promise<BatchResult> {
    if (this.stopped) return { kind: 'retry' };
    if (records.length === 0) return { kind: 'items', items: [] };
    if (this.options.transformer)
      return this.exportCustom(records, this.options.transformer);
    const logs = records.map(readable);
    if (this.native) {
      return new Promise<BatchResult>(resolve => {
        try {
          this.native!.export(logs, result =>
            resolve(
              result.code === ExportResultCode.SUCCESS
                ? {
                    kind: 'items',
                    items: records.map(record => ({
                      eventId: record.eventId,
                      status: 'accepted',
                    })),
                  }
                : { kind: 'retry' }
            )
          );
        } catch {
          resolve({ kind: 'retry' });
        }
      });
    }
    // A caller-supplied mock transport still receives the official OTLP JSON bytes.
    const body = JsonLogsSerializer.serializeRequest(logs);
    if (!body) return { kind: 'retry' };
    try {
      const response = await this.transport.send({
        url: this.options.url,
        body,
        headers: {
          ...this.options.headers,
          'Content-Type': 'application/json',
        },
        timeoutMs: this.timeoutMs,
      });
      if (response.status >= 200 && response.status < 300) {
        return {
          kind: 'items',
          items: records.map(record => ({
            eventId: record.eventId,
            status: 'accepted',
          })),
        };
      }
      return {
        kind: response.status === 429 ? 'pause' : 'retry',
        retryAfterMs: retryAfterMs(response.headers),
      };
    } catch {
      return { kind: 'retry' };
    }
  }

  private async sendCustomRequest(request: HttpRequest): Promise<HttpResponse> {
    const configured = this.options.requestIntervalMs;
    if (configured === undefined) return this.transport.send(request);
    let release!: () => void;
    const turn = new Promise<void>(resolve => {
      release = resolve;
    });
    const previous = this.sendTail;
    this.sendTail = turn;
    await previous;
    try {
      const interval =
        typeof configured === 'function' ? await configured() : configured;
      if (!Number.isFinite(interval) || interval < 0)
        throw new TypeError('requestIntervalMs must be non-negative');
      const wait = Math.max(
        0,
        this.lastRequestStartedAt + interval - Date.now()
      );
      if (wait) await new Promise(resolve => setTimeout(resolve, wait));
      this.lastRequestStartedAt = Date.now();
      return await this.transport.send(request);
    } finally {
      release();
    }
  }

  private async exportCustom(
    records: readonly LogRecordSnapshot[],
    transformer: HttpLogTransformer<Item>
  ): Promise<BatchResult> {
    const encoded = await Promise.all(
      records.map(async record => {
        try {
          return await transformer.encode(record);
        } catch {
          return {
            kind: 'reject' as const,
            eventId: record.eventId,
            reason: 'encode failed',
          };
        }
      })
    );
    const accepted: Item[] = [];
    const rejected: Array<{ eventId: string; status: 'rejected' }> = [];
    encoded.forEach((item, index) => {
      if (item.eventId !== records[index].eventId) {
        rejected.push({ eventId: records[index].eventId, status: 'rejected' });
      } else if ('kind' in item && item.kind === 'reject') {
        rejected.push({ eventId: item.eventId, status: 'rejected' });
      } else {
        accepted.push(item as Item);
      }
    });
    if (accepted.length === 0) return { kind: 'items', items: rejected };
    try {
      const result = await this.sendCustom(accepted, transformer);
      if (result.kind === 'items') {
        const expected = new Set(accepted.map(item => item.eventId));
        const seen = new Set<string>();
        for (const item of result.items) {
          if (!expected.has(item.eventId) || seen.has(item.eventId))
            return { kind: 'retry' };
          seen.add(item.eventId);
        }
        if (seen.size !== expected.size) return { kind: 'retry' };
        return {
          kind: 'items',
          items: [...rejected, ...result.items],
          ...(result.retryAfterMs === undefined
            ? {}
            : { retryAfterMs: result.retryAfterMs }),
          ...(result.retryAfterExact === undefined
            ? {}
            : { retryAfterExact: result.retryAfterExact }),
        };
      }
      return {
        kind: 'items',
        items: [
          ...rejected,
          ...accepted.map(item => ({
            eventId: item.eventId,
            status: 'retryable' as const,
          })),
        ],
        retryAfterMs: result.retryAfterMs,
        ...(result.kind === 'pause' && result.retryAfterMs !== undefined
          ? { retryAfterExact: true }
          : {}),
      };
    } catch {
      return {
        kind: 'items',
        items: [
          ...rejected,
          ...accepted.map(item => ({
            eventId: item.eventId,
            status: 'retryable' as const,
          })),
        ],
      };
    }
  }

  private async sendCustom(
    items: readonly Item[],
    transformer: HttpLogTransformer<Item>
  ): Promise<BatchResult> {
    let request: Awaited<ReturnType<HttpLogTransformer<Item>['buildRequest']>>;
    try {
      request = await transformer.buildRequest(items);
    } catch (error) {
      return error instanceof HttpRequestTooLargeError
        ? this.splitCustom(items, transformer)
        : { kind: 'retry' };
    }
    if (
      this.options.maxRequestBytes !== undefined &&
      request.body.byteLength > this.options.maxRequestBytes
    )
      return this.splitCustom(items, transformer);
    try {
      const response = await this.sendCustomRequest({
        url: this.options.url,
        ...request,
        headers: { ...this.options.headers, ...request.headers },
        timeoutMs: this.timeoutMs,
      });
      if (response.status === 413) return this.splitCustom(items, transformer);
      return await transformer.parseResponse(response, items);
    } catch {
      return { kind: 'retry' };
    }
  }

  private async splitCustom(
    items: readonly Item[],
    transformer: HttpLogTransformer<Item>
  ): Promise<BatchResult> {
    if (items.length === 1)
      return {
        kind: 'items',
        items: [{ eventId: items[0].eventId, status: 'rejected' }],
      };
    const middle = Math.floor(items.length / 2);
    const first = await this.sendCustom(items.slice(0, middle), transformer);
    const rateLimited =
      first.retryAfterMs !== undefined &&
      Number.isFinite(first.retryAfterMs) &&
      first.retryAfterMs >= 0 &&
      (first.kind === 'pause' ||
        (first.kind === 'items' &&
          first.retryAfterExact === true &&
          first.items.some(item => item.status === 'retryable')));
    // A server-directed pause applies to the target; do not send later chunks.
    const second: BatchResult = rateLimited
      ? { kind: 'pause', retryAfterMs: first.retryAfterMs }
      : await this.sendCustom(items.slice(middle), transformer);
    const outcomes = (result: BatchResult, subset: readonly Item[]) =>
      result.kind === 'items'
        ? result.items
        : subset.map(item => ({
            eventId: item.eventId,
            status: 'retryable' as const,
          }));
    const delays = [first.retryAfterMs, second.retryAfterMs].filter(
      (delay): delay is number => delay !== undefined && Number.isFinite(delay)
    );
    return {
      kind: 'items',
      items: [
        ...outcomes(first, items.slice(0, middle)),
        ...outcomes(second, items.slice(middle)),
      ],
      ...(delays.length ? { retryAfterMs: Math.max(...delays) } : {}),
      ...((first.kind === 'pause' && first.retryAfterMs !== undefined) ||
      (second.kind === 'pause' && second.retryAfterMs !== undefined) ||
      (first.kind === 'items' && first.retryAfterExact) ||
      (second.kind === 'items' && second.retryAfterExact)
        ? { retryAfterExact: true }
        : {}),
    };
  }

  async shutdown(): Promise<void> {
    this.stopped = true;
    await this.native?.shutdown();
  }
}
