# Durable OpenTelemetry logs

An opt-in extension for asynchronous OpenTelemetry log delivery with Batch or Durable processing.
Applications register their own filters, HTTP exporters, configuration sources, and
delivery policies. The SDK defines no application events, destinations, or remote
configuration protocol.

## Installation and entry points

After publication, install the package with the compatible OpenTelemetry API and logs SDK:

```sh
npm install opentelemetry-sdk @opentelemetry/api@^1.10.0 @opentelemetry/sdk-logs@3.0.0
```

- Main entry: `opentelemetry-sdk` (`createTelemetry`, registry, emitter, processor, types).
- HTTP entry: `opentelemetry-sdk/exporters/http`.
- Browser storage: `opentelemetry-sdk/web` (IndexedDB).
- Desktop main-process storage: `opentelemetry-sdk/desktop` (async file journal).
- Test-only storage: `opentelemetry-sdk/testing` (memory, not durable).

## Assembly

```ts
import { createTelemetry, type TelemetryConfigSource } from 'opentelemetry-sdk';

const config: TelemetryConfigSource = {
  exporters: [
    { id: 'collector', targetKey: 'collector:v1', type: 'http', url: collectorUrl },
    {
      id: 'partner', targetKey: 'partner:v1', type: 'http', url: partnerUrl,
      transformer: partnerTransformer, configSources: [partnerConfigSource],
    },
  ],
  filter: async record => selectTargets(record), // 0, 1, or several target IDs
  processor: { type: 'batch' },
};
const telemetry = await createTelemetry(config);
const receipt = await telemetry.emit({ eventName: 'example.event', attributes: { value: 1 } });
// With Batch, submitted means accepted into the in-memory processor, not delivered.
await telemetry.forceFlush();
await telemetry.shutdown();
```

The single configuration object chooses one processor and any number of exporter targets.
Only initialized targets selected by the top-level async filter enter the processor.
Missing URLs skip that target; with no valid targets, `emit` is an async no-op.
The built-in `http` exporter sends OTLP JSON unless a caller-supplied transformer changes
the payload. Applications can register new exporter and processor types.

Switch to durable delivery by changing the processor configuration. The browser entry
never imports Node file APIs; desktop storage belongs in the Electron main process.

```ts
import { IndexedDbQueueStorage } from 'opentelemetry-sdk/web';
import { FileQueueStorage } from 'opentelemetry-sdk/desktop';

config.processor = { type: 'durable', options: { storage: new FileQueueStorage(queuePath) } };
// In a web application, use new IndexedDbQueueStorage('telemetry-queue') instead.
// Durable targets also need their own deliveryPolicy.
```

Configuration sources belong to each exporter. Use `initialLoad: 'blocking'` for
local or mock configuration that must be ready before initialization resolves; the default
`'background'` mode keeps remote fetch off the startup critical path. Cached decisions
or the source's explicit fallback apply until a refresh succeeds. Multiple sources
combine by logical AND for `enabled` and union for `disabledEvents`.

An optional asynchronous deliveryPolicySource can override a target's base
policy when runtime conditions change. Its subscribe callback prompts the
processor to reload the policy and reschedule the next batch. Applications
can supply network-quality-dependent count and timing limits without teaching
the SDK about any particular network classification.

The base policy can optionally define retryDelaysMs (a per-target failure
ladder whose last step repeats), retryJitterMode set to symmetric, and
queueEviction with a highWatermark and fraction. Without these fields, the
existing exponential retry and hard queue-capacity behavior remains. Valid
server-directed rate-limit delays take precedence without advancing the local
failure ladder; accepted batches reset it, while partial retryable or wholly
rejected batches do not. Queue eviction removes oldest non-leased records and
keeps the journal's rejected-record history bounded.

The optional custom HTTP transformer can throw HttpRequestTooLargeError, or
the exporter can enforce maxRequestBytes. Oversized batches are split; a
single item that still exceeds the limit, or receives HTTP 413, is rejected
without blocking other items. Other transformer errors remain retryable.
Custom HTTP requests may also use a number or asynchronous requestIntervalMs
callback to pace every request, including those created by a split batch.

## Verification and publication

This package is versioned independently from its sibling packages in this fork.
Build, test, lint, and validate its
published entry points through the repository workflows: `npm run compile`,
`npm run test`, `npm run test:browser`, `npm run lint`, and the
`verify-pack` CI job. The last job runs the repository's own artifact and
conditional-export checks after compilation.

The preferred release path is the repository release PR and `publish-to-npm`
workflow. When the npm owner uses security-key authentication for this fork,
`npm run publish:local -- packages/durable-logs` provides the interactive
fallback. Bump this package's version and lockfile, verify the package, and
publish only the reviewed commit.

`forceFlush()` waits for queued records to receive terminal acknowledgements
or rejects on timeout. `shutdown()` attempts the same flush before closing
storage; when a destination is unavailable, unacknowledged records remain in
the durable queue for a later start.
