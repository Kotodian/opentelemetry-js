/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

import { diag } from '@opentelemetry/api';
import type { Resource } from '@opentelemetry/resources';
import { LoggerProvider } from '@opentelemetry/sdk-logs';
import type {
  BatchLogRecordProcessorOptions,
  LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import { BatchConfigState } from './BatchConfigState';
import { BatchDispatchProcessor } from './BatchDispatchProcessor';
import { DurableDispatchProcessor } from './DurableDispatchProcessor';
import type { DurableDispatchOptions } from './DurableDispatchProcessor';
import { ExporterRegistry } from './ExporterRegistry';
import { TelemetryEmitter } from './TelemetryEmitter';
import { HttpLogExporter } from './exporters/http';
import type { EncodedHttpItem, HttpLogExporterOptions } from './exporters/http';
import type {
  DeliveryPolicy,
  DeliveryPolicySource,
  DurableLogExporter,
  EmitInput,
  EmitReceipt,
  ExporterConfigSource,
  PreparedLogRecord,
} from './types';

export interface ExporterDefinition
  extends Partial<HttpLogExporterOptions<EncodedHttpItem>> {
  id: string;
  targetKey: string;
  type: string;
  configSources?: readonly ExporterConfigSource[];
  deliveryPolicy?: DeliveryPolicy;
  deliveryPolicySource?: DeliveryPolicySource;
}

export interface TelemetryConfigSource {
  resource?: Resource;
  exporters: readonly ExporterDefinition[];
  filter(record: Readonly<PreparedLogRecord>): Promise<readonly string[]>;
  processor:
    | {
        type: 'batch';
        options?: Omit<BatchLogRecordProcessorOptions, 'exporter'>;
      }
    | { type: 'durable'; options: Omit<DurableDispatchOptions, 'registry'> }
    | { type: string; options?: unknown };
}

export interface Telemetry {
  emit(input: EmitInput): Promise<EmitReceipt | undefined>;
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
}

export type ExporterFactory = (
  definition: Readonly<ExporterDefinition>
) => Promise<DurableLogExporter | undefined>;
export type ProcessorFactory = (
  registry: ExporterRegistry,
  options: unknown
) => Promise<LogRecordProcessor & { start?(): Promise<void> }>;

const exporterFactories = new Map<string, ExporterFactory>([
  [
    'http',
    async definition =>
      definition.url
        ? new HttpLogExporter({
            url: definition.url,
            headers: definition.headers,
            transformer: definition.transformer,
            transport: definition.transport,
            timeoutMs: definition.timeoutMs,
            maxResponseBytes: definition.maxResponseBytes,
            maxRequestBytes: definition.maxRequestBytes,
            requestIntervalMs: definition.requestIntervalMs,
          })
        : undefined,
  ],
]);

const processorFactories = new Map<string, ProcessorFactory>([
  [
    'batch',
    async (registry, options) =>
      new BatchDispatchProcessor(
        registry,
        options as Omit<BatchLogRecordProcessorOptions, 'exporter'> | undefined
      ),
  ],
  [
    'durable',
    async (registry, options) =>
      new DurableDispatchProcessor({
        ...(options as Omit<DurableDispatchOptions, 'registry'>),
        registry,
      }),
  ],
]);

/** Register a component type; initialization still accepts only one ConfigSource. */
export function registerExporterType(
  type: string,
  factory: ExporterFactory
): void {
  if (!type || exporterFactories.has(type))
    throw new Error(`duplicate exporter type: ${type}`);
  exporterFactories.set(type, factory);
}

export function registerProcessorType(
  type: string,
  factory: ProcessorFactory
): void {
  if (!type || processorFactories.has(type))
    throw new Error(`duplicate processor type: ${type}`);
  processorFactories.set(type, factory);
}

/** Async component assembly; no destination means an async no-op emitter. */
export async function createTelemetry(
  config: TelemetryConfigSource
): Promise<Telemetry> {
  if (typeof config.filter !== 'function')
    throw new TypeError('filter is required');
  const processorFactory = processorFactories.get(config.processor.type);
  if (!processorFactory)
    throw new Error(`unknown processor type: ${config.processor.type}`);
  const ids = new Set<string>();
  for (const definition of config.exporters) {
    if (!definition.id || ids.has(definition.id) || !definition.targetKey)
      throw new TypeError(
        'exporter id and targetKey must be unique and nonempty'
      );
    ids.add(definition.id);
    if (!exporterFactories.has(definition.type))
      throw new Error(`unknown exporter type: ${definition.type}`);
    if (config.processor.type === 'durable' && !definition.deliveryPolicy)
      throw new TypeError(
        `durable exporter ${definition.id} needs deliveryPolicy`
      );
  }

  const batchConfig =
    config.processor.type === 'batch'
      ? new BatchConfigState(config.exporters)
      : undefined;
  const registry = new ExporterRegistry(
    5000,
    config.filter,
    ids,
    (id, eventName) => batchConfig?.isAllowed(id, eventName) ?? true
  );
  const created = await Promise.all(
    config.exporters.map(async definition => {
      try {
        const exporter = await exporterFactories.get(definition.type)!(
          definition
        );
        return exporter ? { definition, exporter } : undefined;
      } catch (error) {
        diag.warn(`exporter ${definition.id} could not be created`, error);
        return undefined;
      }
    })
  );
  for (const target of created) {
    if (!target) continue;
    registry.register({
      id: target.definition.id,
      targetKey: target.definition.targetKey,
      exporter: target.exporter,
      deliveryPolicy: target.definition.deliveryPolicy,
      deliveryPolicySource: target.definition.deliveryPolicySource,
      configSources: target.definition.configSources,
    });
  }
  if (registry.getRegistrations().length === 0)
    return {
      async emit() {
        return undefined;
      },
      async forceFlush() {},
      async shutdown() {},
    };

  try {
    await Promise.all([registry.initialize(), batchConfig?.start()]);
  } catch (error) {
    batchConfig?.shutdown();
    await registry.shutdown();
    throw error;
  }
  let processor: LogRecordProcessor & { start?(): Promise<void> };
  try {
    processor = await processorFactory(registry, config.processor.options);
    await processor.start?.();
  } catch (error) {
    batchConfig?.shutdown();
    await registry.shutdown();
    throw error;
  }
  const provider = new LoggerProvider({
    resource: config.resource,
    processors: [processor],
  });
  const emitter = new TelemetryEmitter(
    provider.getLogger('opentelemetry-sdk'),
    registry
  );
  let stopping: Promise<void> | undefined;
  return {
    emit: input => emitter.emit(input),
    forceFlush: () => provider.forceFlush(),
    shutdown: () =>
      (stopping ??= (async () => {
        try {
          await provider.shutdown();
        } finally {
          batchConfig?.shutdown();
          await registry.shutdown();
        }
      })()),
  };
}
