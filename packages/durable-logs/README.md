# Durable OpenTelemetry logs

An opt-in extension for asynchronous, durable delivery of OpenTelemetry log events.
Applications register their own filters, HTTP exporters, configuration sources, and
delivery policies. The SDK defines no application events, destinations, or remote
configuration protocol.

## Installation and entry points

After publication, install the package with the compatible OpenTelemetry API and logs SDK:

```sh
npm install opentelemetry-sdk @opentelemetry/api@^1.10.0 @opentelemetry/sdk-logs@3.0.0
```

- Main entry: `opentelemetry-sdk` (registry, emitter, processor, types).
- HTTP entry: `opentelemetry-sdk/exporters/http`.
- Browser storage: `opentelemetry-sdk/web` (IndexedDB).
- Desktop main-process storage: `opentelemetry-sdk/desktop` (async file journal).
- Test-only storage: `opentelemetry-sdk/testing` (memory, not durable).

## Assembly

```ts
import { LoggerProvider } from '@opentelemetry/sdk-logs';
import {
  DurableDispatchProcessor, ExporterRegistry, TelemetryEmitter,
} from 'opentelemetry-sdk';
import { HttpLogExporter } from 'opentelemetry-sdk/exporters/http';
import { IndexedDbQueueStorage } from 'opentelemetry-sdk/web';

const registry = new ExporterRegistry();
registry.register({
  id: 'destination-a',
  targetKey: targetIdentity, // stable across restarts, unique to environment and destination
  filter: async record => shouldDeliver(record),
  exporter: new HttpLogExporter({ url: destinationUrl }), // default OTLP JSON
  deliveryPolicy, // application-provided complete DeliveryPolicy
});
await registry.initialize();

const processor = new DurableDispatchProcessor({
  storage: new IndexedDbQueueStorage('telemetry-queue'),
  registry,
});
const provider = new LoggerProvider({ processors: [processor] });
await processor.start();
const emitter = new TelemetryEmitter(provider.getLogger('events'), registry);
const receipt = await emitter.emit({ eventName: 'example.event', attributes: { value: 1 } });
// receipt.status === 'queued' means durable local commit, not remote acknowledgement.

await provider.shutdown();
await registry.shutdown();
```

For a desktop application, construct `FileQueueStorage(path)` in the main process and
import it from the `/desktop` entry. The browser entry never imports Node file APIs.

```ts
import { FileQueueStorage } from 'opentelemetry-sdk/desktop';

const desktopProcessor = new DurableDispatchProcessor({
  storage: new FileQueueStorage(queuePath),
  registry,
});
```

Register another `HttpLogExporter` with a caller-provided transformer to send a
different wire format. A registration's async filter decides which records it receives;
the transformer only controls that exporter's HTTP payload and response parsing.

Configuration sources belong to each registration. Use `initialLoad: 'blocking'` for
local or mock configuration that must be ready before `start()` resolves; the default
`'background'` mode keeps remote fetch off the startup critical path. Cached decisions
or the source's explicit fallback apply until a refresh succeeds. Multiple sources
combine by logical AND for `enabled` and union for `disabledEvents`.

## Verification and publication

This package lives in the repository's stable SDK release group and uses the
same version as its sibling packages. Build, test, lint, and validate its
published entry points through the repository workflows: `npm run compile`,
`npm run test`, `npm run test:browser`, `npm run lint`, and the
`verify-pack` CI job. The last job runs the repository's own artifact and
conditional-export checks after compilation.

Maintainers publish through the repository release PR and
`publish-to-npm` workflow. The npm package owner must authorize the first
publication and configure trusted publishing for this fork before the
workflow can publish this package. Do not publish from a development machine.

`forceFlush()` waits for queued records to receive terminal acknowledgements
or rejects on timeout. `shutdown()` attempts the same flush before closing
storage; when a destination is unavailable, unacknowledged records remain in
the durable queue for a later start.
