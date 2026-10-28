# OpenTelemetry 持久化日志投递扩展设计

状态：设计评审稿（2026-10-28）。基于本仓库 OpenTelemetry JS，只**新增**事件入口、一个持久化调度 processor、一个带可选 `transformer` 的通用 HTTP 日志 exporter，以及 Web／桌面存储适配；不修改上游 `Logger`、`LoggerProvider`、已有 processor／exporter。未传 `transformer` 时默认使用本仓库的 `@opentelemetry/exporter-logs-otlp-http` 发送 OTLP 日志；传入时由调用方定义 HTTP 请求和响应格式。SDK 不内置任何目标类型或事件分类。公开包不包含接入方的地址、凭据、事件名单、私有协议映射或内部文档；这些由调用方在包外注入。

**现有源码边界：** `api/src/logs/Logger.ts` 的 `emit(): void`、`packages/sdk-logs/src/LogRecordProcessor.ts` 的 `onEmit(): void`、`packages/sdk-logs/src/export/LogRecordExporter.ts` 的回调式 `export(..., callback): void` 均保持原样。`LoggerProvider` 已有 `resource`／`processors` 装配点；`experimental/packages/exporter-logs-otlp-http` 已提供浏览器和 Node 的 `OTLPLogExporter`，但构造函数固定使用 `JsonLogsSerializer`，配置仅能改变 URL、头等传输参数，无法改变请求体格式。底层 `ISerializer` 可替换字节序列化，但现有浏览器传输丢弃响应体，现有成功回调也不表达逐项结果；自定义协议因此需要新增通用 HTTP 扩展，不能只改现有 exporter 配置。本仓库目前没有持久队列或下文的 `TelemetryEmitter`／`ExporterRegistry`／`HttpLogExporter`；以下接口均为拟新增扩展。`BatchLogRecordProcessorBase.ts` 的现有默认值仅是批次 512、内存队列 2048、等待 1000 ms、导出超时 30000 ms；不能把这四项说成持久化重试或 HTTP 调度的默认值。

## 1. 装配：processor 与 exporter 全部注册

filter 是 exporter 注册表中的准入条件，与 processor 分离。接入业务在 SDK 外定义 filter，注册 exporter 时一起传入；`TelemetryEmitter` 在调用 OTel `Logger.emit` **之前**运行 filter 并检查命中 exporter 是否已初始化。没有命中，或命中的 exporter 全部未初始化，就不调用 `Logger.emit`，因此不会进入任何 processor。`DurableDispatchProcessor` 不注册、不执行 filter，只负责已准入事件的持久化队列和调度。

OpenTelemetry 仍按现有机制在 `LoggerProvider.processors` 注册 processor。`DurableDispatchProcessor` 放在需要修改 LogRecord 的其他 processor **之后**，仅对已准入的记录取得最终快照；前置 processor 的改写不重新触发 filter。

```ts
// 浏览器装配示例：目标、filter、配置来源和自定义格式全部由调用方提供。
const secondaryConfigSource: ExporterConfigSource = {
  id: 'remote',
  fallback: { enabled: true, refreshAfterMs: remoteDefaultRefreshMs },
  load: async () => fetchNormalizedConfigDecision(),
  refreshJitterRatio: 0.3,
};

const exporters = new ExporterRegistry();
exporters.register({
  id: 'primary',
  targetKey: primaryTargetKey,
  filter: filters.primary,
  exporter: new HttpLogExporter({ url: primaryUrl }), // 未传 transformer：OTLP
  deliveryPolicy: primaryDeliveryPolicy,
});
exporters.register({
  id: 'secondary',
  targetKey: secondaryTargetKey,
  filter: filters.secondary,
  exporter: new HttpLogExporter({
    url: secondaryUrl,
    transformer: secondaryTransformer,
  }),
  deliveryPolicy: secondaryDeliveryPolicy,
  configSources: [secondaryConfigSource],
});
await exporters.initialize();
const delivery = new DurableDispatchProcessor({
  storage: new IndexedDbStorage('otel-durable-logs'), // 桌面改用主进程文件存储
  resolveTarget: (id, targetKey) =>
    exporters.getInitializedTarget(id, targetKey),
  subscribeTargetAvailability: wake => exporters.subscribeAvailability(wake),
});
const provider = new LoggerProvider({
  resource,
  processors: [...registeredTransformProcessors, delivery],
});
await delivery.start();
const emitter = new TelemetryEmitter(
  provider.getLogger('app-events'),
  exporters
);
```

SDK 不定义事件的业务类型、目标归属或固定属性名。调用方自定义 LogRecord 字段和各目标 filter；两个 filter 都返回 true 且两个 exporter 都已初始化时，同一事件分别投递给两个独立注册项。`transformer` 仅定义单个 HTTP 目标的传输格式，不决定事件是否命中该目标。

```ts
interface ExporterRegistration {
  id: string; // 同一 ExporterRegistry 内稳定且唯一
  targetKey: string; // 环境 + 目标身份，重启后仍可辨认
  filter: (record: Readonly<PreparedLogRecord>) => Promise<boolean>;
  exporter: DurableLogExporter;
  deliveryPolicy: DeliveryPolicy; // 新 processor 的完整策略，由宿主逐目标提供
  configSources?: readonly ExporterConfigSource[]; // 宿主按目标组合配置来源
}

// ExporterRegistry 对外暴露 register(registration)。
```

配置来源属于**单个 exporter 注册项**，不由 SDK 固定为环境变量或某个 HTTP 接口。调用方可给任意目标注册环境变量、远程、本地或测试 mock 来源，也可组合多个来源。每个来源有稳定 `id`、显式 `fallback`、异步 `load()` 和可选变更订阅。`load()` 的 Promise 返回通用决定；每个来源分别缓存最近有效结果，失败沿用缓存或 fallback。多个来源时 `enabled` 取逻辑与、`disabledEvents` 取并集；没有来源的目标默认可投递。配置来源与 `transformer` 相互独立，配置状态不改变 exporter 实例和旧队列的身份。

注册列表可以包含任意多个 exporter，甚至多个同类型 HTTP 目标。SDK 只按注册项 `id + targetKey` 隔离队列、配置缓存、调度状态和 ACK；不假设固定的目标组合。每个目标的 filter 独立判断，同一 LogRecord 命中几个可用目标就原子写入几个投递项。`deliveryPolicy` 是这个新增持久 processor 的逐目标参数，不能把现有内存批处理器的四个默认值直接当成全部持久化策略；调用方装配时须给出完整对象。

`ExporterRegistry` 在 `TelemetryEmitter.emit()` 中并发等待各注册项的异步、只读、无副作用 filter，再从命中项中选出已初始化的 exporter。filter 返回 `Promise<boolean>`，不得修改记录或发网络请求；异步仅用于避免把调用方逻辑锁在 OTel 的同步回调中。零命中返回 `unmatched`；命中了但没有已初始化的 exporter 返回 `unavailable`。两种情况都不调用 `Logger.emit`，也不让 processor 处理。若只有部分命中项已初始化，仅这些目标进入后续处理，未初始化的目标不入队。任一 filter 拒绝或超时，本次准入失败，包装入口 reject，仍不调用 `Logger.emit`。

注册只允许在 `ExporterRegistry.initialize()` 前完成；重复 exporter ID、空 filter 或无目标身份直接拒绝注册。`initialize()` 对各 exporter 独立等待并返回逐目标结果：成功的实例标记为可用，单个失败不阻止其他目标启动；失败的实例可单独重试初始化，期间不会被新事件准入。registry 在目标初始化成功、失效或重新初始化时通知 processor 唤醒该目标的已有积压。`resolveTarget(id, targetKey)` 必须同时核对目标身份，不能把旧环境的队列交给同 ID 的新目标。存储不可用则由 `DurableDispatchProcessor.start()` 报错。`targetKey` 包含环境与目标身份，队列按 `exporterId + targetKey` 隔离；宿主换环境后，旧目标的积压保持待处理，不会用新凭据发送，须由同一目标重新注册或显式迁移／丢弃。Web 包只加载 IndexedDB 适配，桌面包只加载主进程文件适配，不把 Node／Electron 依赖带进 Web。

## 2. emit：统一的 LogRecord 输入

调用方的事件规则决定触发时机、事件名、属性和 body；SDK 不定义分类标签。`TelemetryEmitter` 补事件 ID 与发生时间，形成不可变的 `PreparedLogRecord`，先交给独立的 `ExporterRegistry` 做 filter／初始化状态准入；只有存在可用目标时才调用原生 `Logger.emit`。这里处理的是日志事件，不创建另一套 OTel Meter 指标管线。emit 不指定 exporter，同一事件也不因可能去两个目标而创建两种形状。

| LogRecord 字段           | 统一语义                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------------ |
| `eventName`／`timestamp` | 业务事件名与发生时刻；exporter 各自转换为目标时间格式。                                    |
| `attributes`             | 调用方属性；`event.name`、`event.id` 为稳定索引／去重字段，其他字段由调用方定义。          |
| `body`                   | 原始事件内容；由目标 HTTP exporter 的默认 OTLP 格式或可选 transformer 编码。               |
| `resource`／`scope`      | 由 LoggerProvider 提供给后续 processor／exporter；准入 filter 只读取 `PreparedLogRecord`。 |

调用方可在 emit 时提供自己的上下文；SDK 只冻结记录快照，不要求特定设备、账号、渠道或平台字段。可选 transformer 从快照校验并编码目标需要的格式；某条记录无法编码时仅拒绝该目标的投递项，不影响其他已命中目标。地址、鉴权材料和格式规则由调用方配置，不进入公开包。事件 ID、发生时间与原始内容在网络重试中保持不变。

```ts
// 新增包的异步入口；不修改原生 Logger.emit 的签名。
await emitter.emit({
  eventName,
  timestamp: occurredAtMs,
  attributes: callerAttributes,
  body: callerBody,
});
// 仅在至少一个命中 exporter 已初始化时才调用 logger.emit(...)。
```

`TelemetryEmitter.emit(): Promise<EmitReceipt>` 是受持久投递管理的唯一入口：它在准入前生成非空 `event.id`，filter 读取的是此时的 `PreparedLogRecord`，而非后续 processor 修改过的记录。原生 `Logger.emit` 和 `LogRecordProcessor.onEmit` 均返回 `void`，无法直接给调用方“已落盘”的 Promise；直接绕过 `TelemetryEmitter` 的原生调用不参与本扩展的目标选择或持久投递。

**异步边界。** 调用方异步提交事件，不等待网络 ACK，也不让埋点失败改变原操作结果。SDK 新增的 `initialize()`、`start()`、`emit()`、filter、`forceFlush()`、`shutdown()`、配置 `load()`、存储读写／落盘、transformer 的编码／请求构造／响应解析和 HTTP 发送均返回 Promise。上游 OTel 的 `Logger.emit()`／`onEmit()` 签名仍是同步 `void`，无法改变；在异步 filter 完成后，这两个同步桥接只传递投递计划和拍有界快照，不做文件 I/O、加密／压缩或网络请求。耗时的序列化、压缩和加密不能仅包装进 Promise 后继续占用 UI／Electron 主线程；实现须采用浏览器 Worker 或桌面 worker 执行，并限制单批大小和并发。`onEmit()` 将快照与本次回执关联，异步持久化事务提交后才 resolve `emit()`；失败 reject。启动前事件由调用方有界缓冲并保留原发生时间，待 `start()` 完成后再异步入队；缓冲中的事件不得提前宣称 `queued`，初始化失败须明确 reject／报告丢弃。这里描述的是拟实现的目标状态，不代表各仓库现有埋点链路已全部改成异步。

准入成功后，emitter 把已初始化且命中的目标 ID、`targetKey` 和事件 ID 放入本次 `Logger.emit` 的 OTel `Context`，再调用 `Logger.emit`。`PreparedLogRecord` 是补齐事件 ID、时间和身份上下文的普通 OTel `LogRecord` 输入，filter 仅能读取它的处理前字段；后续 `onEmit` 才获得 SDK 创建的 `ReadWriteLogRecord`。`DurableDispatchProcessor.onEmit(logRecord, context)` 只读取已确定的投递计划并拍最终快照；它不持有 filter，也不重新匹配 exporter。若 `Context` 没有投递计划，该 processor 不入队。事件 ID 是保留字段，前置 processor 不得改写；快照 ID 与计划不一致时拒绝入队并报错。

```ts
interface EmitReceipt {
  eventId: string;
  matched: string[]; // filter 命中的 exporter ID
  unavailable: string[]; // 命中但尚未初始化的 exporter ID
  queued: string[]; // 已持久化的目标；网络尚未确认
  suppressed: Array<{ exporterId: string; reason: string }>; // 仅该目标的组合配置
  status: 'queued' | 'suppressed' | 'unmatched' | 'unavailable';
}
```

零 filter 命中时返回 `unmatched`；有命中但无已初始化 exporter 时返回 `unavailable`。这两种回执都在调用 `Logger.emit` 前完成，`queued` 为空。若有可用目标，`onEmit` 同步拍快照，异步把所有允许投递的目标在同一存储事务中入队；提交后才 resolve 回执，失败则 reject，不能出现“一个目标已入队、另一个未入队，却返回成功”。全部可用目标被组合配置开关抑制时返回 `suppressed`；若有目标入队则返回 `queued`，同时列出不可用和被抑制的目标。若 logger 被禁用、前置 processor 抛错，或已调用 `Logger.emit` 却未进入持久化 processor，包装入口失败并清除回执。

## 3. 单个 processor：持久化队列和调度

`DurableDispatchProcessor` 是唯一新增的投递 processor。它只接收 emitter 已准入的目标列表，不参与 filter 定义或匹配。`onEmit(ReadWriteLogRecord, Context)` 在当前调用栈中读取投递计划，并复制 `eventName`、`hrTime`、`hrTimeObserved`、`attributes`、`body`、`resource.attributes`、`instrumentationScope`、severity 等需要异步使用的值；上游源码允许在 onEmit 期间修改记录，但回调返回后异步处理只能读取，因此**不能把可变 LogRecord 对象直接留在队列里**。快照序列化时拒绝无法持久化的值，保留 `event.id` 作为所有目标的稳定去重键。

```text
TelemetryEmitter → ExporterRegistry 运行 filter 并核对初始化状态
  → 无可用 exporter：返回，不调用 Logger.emit，也不进入 processor
  → 有可用 exporter：把目标列表放入 Context，再调用 Logger.emit
  → 前置已注册 processor → DurableDispatchProcessor.onEmit
  → 读取目标列表，拍不可变 LogRecordSnapshot，检查各目标组合配置
  → 同一存储事务 appendFanout 所有允许投递的 (eventId, exporterId, targetKey, snapshot)
  → 返回本地落盘回执；调度器按目标唤醒
```

队列**归 processor 所有**，不是独立的顶层服务，也不由 exporter 持有。每个投递项保存事件快照、`exporterId`、`targetKey`、持久顺序号、入队时间、尝试次数、下次重试截止时间和在途租约。相同 LogRecord 命中两个 exporter 时形成两个独立投递项；一个目标的发送失败不阻塞另一个。以 `(event.id, exporterId, targetKey)` 为幂等键；同键同快照可安全重试，同键不同快照视为冲突而拒绝。存储适配必须支持跨目标的原子 `appendFanout`；后续领取、结算及配置更新按目标分区事务处理：

| 操作           | 必须保证的结果                                                                                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `appendFanout` | 对同一 LogRecord 的多个已准入目标原子写入；事务提交才确认 emitter 回执。策略在写入事务里再检查一次，避免准入后开关恰好关闭。                            |
| `claim`        | 原子取得目标分区租约和递增 fencing token，按队首取批并核对当前策略；同一目标最多一个在途请求。若 Web 另一标签页先领取，放弃本次编码后重选。             |
| `settle`       | 只接受当前 fencing token 的结算；仅删除明确 accepted；rejected 移到有界错误记录；retryable／未知状态保留，原子保存退避截止或完整 ACK 后的批次间隔起点。 |
| `recover`      | 重启恢复队列、排空状态、失败次数、下次发送时间和逐来源配置；未完成结算的在途项以原 ID 重试。                                                            |

Web 适配通过异步 IndexedDB 事务写入所有命中目标，并用跨标签页租约领取；桌面只在主进程运行，把同一次 fan-out 编成一个带长度／校验和的文件日志帧，异步写入并等待该帧的 `FileHandle.sync()` 完成才确认落盘，重启重放至最后一个完整帧。桌面存储只使用 Promise 形式的文件操作，不在事件路径调用 `mkdirSync`、`readFileSync`、`writeFileSync` 或 `fsyncSync`。两端只替换 processor 的存储适配，filter、选批和 exporter 接口不变。存储满或失败时不能虚报入队成功。`shutdown` 等待进行中的写入与 ACK 结算；网络请求发出不等于确认。

### 启动：初始化队列

`delivery.start()` 必须在 exporter 注册／首次初始化尝试之后、向业务暴露 emitter 之前完成。processor 生命周期为 `new → starting → running → stopping → stopped`；并发 `start()` 共享同一个启动 Promise，已运行时重复调用直接返回，不重复打开存储或创建调度器。启动失败进入 `stopped`，清理监听和句柄后才允许重试。其执行顺序如下：

1. 打开 Web 的 IndexedDB 或桌面主进程文件存储，检查 schema 版本并执行可回滚的迁移；打开或迁移失败则 `start()` reject，业务不能把事件误认为已持久化。
2. 读取持久化的队列分区、顺序号、重试截止、排空状态、未完成租约、错误记录和每个目标各配置来源的缓存。按 `(exporterId, targetKey)` 还原队列；未知或尚未初始化的目标保留原队列，但不领取和发送。不得因本次注册列表变化清空旧目标数据。
3. 对已过期的在途租约恢复待重试状态，保留原事件 ID、原时间与失败次数；未过期的租约等待到期或由当前持有者结算，避免 Web 多标签页重复抢占。重放只接受完整提交的 fan-out 事务。
4. 为每个已注册目标建立独立调度状态：`timer`、`inFlight`、`lastTerminalAckAt`、`drainActive` 和各配置来源的刷新截止；装入注册时的 `deliveryPolicy`。先异步读取逐来源持久缓存或显式 fallback，计算组合开关；再调用每个来源的 `load()`，本地／mock 来源的 Promise 在 `start()` 完成前结算，需要网络的来源在后台刷新，不阻塞 `start()`。先订阅在线状态、registry 可用性变化、配置来源变化和存储变更，再进行首次调度，避免初始化与首次调度之间漏掉唤醒。旧队列中未注册的目标只保留数据，不创建发送定时器。
5. 对已有积压的可用目标立即尝试一次调度判断；判断仍受组合开关、最短发送间隔和已保存的退避截止约束。`start()` 完成表示队列已恢复且调度器可工作，不表示积压已经送达。

队列首次启动为空时写入存储 schema／元数据即可；已有队列只读恢复，不能在启动时把历史记录重新 emit。每个目标分区至少持久化队列项、单调顺序号、租约及 fencing token、重试截止、`drainActive`、最近一次完整 ACK 的结算时间、逐来源配置版本与下次刷新时间。`start()` 不等待网络配置拉取完成；刷新到期只唤醒该来源的配置任务，发送沿用该来源已缓存或注册 fallback 的决定。

### 运行：每个目标独立调度

`deliveryPolicy` 由注册者提供 `triggerCount`、`maxWaitMs`、`maxBatchCount`、`maxBatchBytes`、`minIntervalMs`、退避／抖动、队列容量／高水位淘汰和保留期限；可另外注入网络状态到弱网策略的映射，下一批才切换参数。processor 不按 exporter 名称内置某个平台的数值。每个 `(exporterId, targetKey)` 同时最多一个在途批次，不同目标可并行。`appendFanout` 提交、定时器到期、网络恢复、exporter 初始化成功、配置来源变化及 `forceFlush` 都只唤醒受影响的目标。

每个目标只有一个合并唤醒的调度循环，不为每条事件创建定时器。`wake(target)` 把同一轮收到的 append、定时器、网络、配置和初始化通知合并；循环每次都重新读取存储，旧 timer 作废。队列按持久顺序号处理：队首未确认项若仍在租约或退避中，后面的项不能越过它发送。队首可发送且数量达到 `triggerCount` 或入队等待达到 `maxWaitMs` 时，在事务中置 `drainActive=true`；随后连续取批直到排空，不对剩余批次重新套用首批门槛。网络恢复且有积压时同样进入排空；重启时恢复已触发的排空状态。`forceFlush` 只对本轮可发送批次跳过首批门槛，不把未触发的队列永久改成排空状态。每次发下一批仍受 `minIntervalMs`、重试截止和配置开关限制。离线、组合开关关闭或 exporter 不可用时不反复发送，仅保留配置来源刷新 timer，并等待状态变化。Web 多标签页收到存储变更后各自重新判断，但只有持有分区租约的标签页能发送。

以下伪代码是 processor 内部的调度契约；`inspect`、`claim`、`settle` 都按 `(exporterId, targetKey)` 操作，不执行 filter：

```text
wake(target, force = false):
  合并同目标唤醒（保留 pendingForce）；若该目标正在 drive，记 pendingWake 待结算后重查
  否则启动 drive(target, force)；不同目标可并行

drive(target, force):
  读取最新队列、组合配置、drainActive、租约、重试时间和上次完整 ACK 时间
  如果目标不可用、离线或组合开关关闭：暂停发送，等待状态变化／配置刷新
  如果已有在途批次：等待结算或租约到期，不启动第二个请求
  移除已被配置禁用、已过保留期的非在途队首项；队列空则清除 drainActive
  如果队首在租约或退避中：设置其截止唤醒，返回（不能跳过队首）
  如果 force 或 drainActive 或 网络恢复补传 或 条数 >= triggerCount：batchDue = now
  否则 batchDue = 队首.enqueueAt + maxWaitMs
  sendDue = max(batchDue, lastTerminalAckAt + minIntervalMs, now)
  如果 sendDue > now：保留 pendingForce，设置该时刻及更早的新项／配置变化唤醒，返回
  在存储事务内重新核对组合开关与截止；首批触发时保存 drainActive
  按序 claim 一批并取得 fencing token
  编码并按 maxBatchBytes 缩批；释放未发送项的租约
  在事务外发送；用当前 fencing token 按逐项 ACK settle
  若还有积压，按批次间隔／退避重新判断；排空后清除 drainActive
```

各配置来源刷新与事件发送共用目标状态，但使用各自的到期时间和失败退避；刷新结果持久化并重新计算组合开关后再唤醒该目标。任何状态变化都必须重新计算计时器，而不是沿用旧配置下的到期时间。租约到期后，旧持有者的 `settle` 因 fencing token 失效而被拒绝，未明确 ACK 的条目以原事件 ID 再次领取。

调度器每次唤醒按以下顺序执行，而不是在 `onEmit` 中直接发网络请求：

1. 先核对 exporter 初始化、网络和所有配置来源的组合开关，再核对队首租约及重试截止；队首阻塞时只安排其到期唤醒，不跳过它。数量门槛统计同一目标未确认、未在途的有效事件，不能把当前在途项重复计数。
2. 在存储事务中按持久顺序领取至多 `maxBatchCount` 条，写入租约；调用 exporter 编码检查最终请求字节数，超过 `maxBatchBytes` 就缩批，并立即释放未发送项的租约；单条仍超限则隔离该项并继续。Web 其他标签页已领取时本轮放弃，不发送未持有租约的条目。
3. 在事务外发送批次；在 `settle` 事务中仅删除明确 accepted 的条目，永久 rejected 移入有界错误记录，retryable／超时／未知 ACK 保留原快照并持久化次数及下次重试时间。有效的服务端限流截止优先于本地退避；部分 ACK 中仍有 retryable 时暂停后续批次，不重置失败计数。完整有效 ACK 且该批无 retryable 时原子记录 `lastTerminalAckAt`，全 rejected 也需要批次间隔但不算成功、不清零失败计数。ACK 结算提交后才能发该目标下一批。
4. 根据队首等待、重试截止、成功批次间隔、租约到期和各配置来源刷新时间设置该目标下一次必要唤醒。容量淘汰与过期处理按该目标政策在存储事务内执行，跳过在途项，不改旧事件时间伪装新事件；目标队列排空才清除 `drainActive`。

`forceFlush()` 绕过条数／队首等待门槛并唤醒目标，但不绕过组合开关、exporter 未初始化、离线、服务端限流或已持久化的退避截止；其 `pendingForce` 保留到本轮发送或超时，不会因最短间隔到期前的定时等待丢失。它等待本轮可发送批次的结算或超时，不承诺整个队列已被服务器接收。`shutdown()` 停止新的入队和定时器，等待正在进行的 append 与 ACK 结算落盘后关闭存储；未发送项保留到下次 `start()` 恢复。

## 4. exporter 的输入、结果和远程配置

注册时的 `filter` 由 `ExporterRegistry` 在 processor 之前独立执行；exporter 的编码、发送、解码接口都返回 Promise，processor 在目标的异步调度循环中等待结果。exporter 不删队列、不启动自己的重试时钟，也不使用同步加密、压缩或文件 API。发送契约返回每个投递项的 `accepted`／`retryable`／`rejected`，或请求级 `retry`／`pause`；响应项数或 ID 不对应整批输入时视为未知，processor 保留原项。发送成功但本地 ACK 事务前崩溃可能重发，这是至少一次投递，不宣称网络恰好一次。

**唯一的 `HttpLogExporter` 类型。** 这是拟新增的 Web／Node 通用扩展；构造参数为目标 `url`、标准 HTTP 配置和**可选** `transformer`、`transport`。未传 `transformer` 时，把持久快照恢复为 `ReadableLogRecord`，默认复用本仓库已有的 `@opentelemetry/exporter-logs-otlp-http` `OTLPLogExporter`，沿用 OTLP JSON、默认 `Content-Type` 和标准 HTTP 行为；URL 由调用方传入。传入 `transformer` 时，同一个 `HttpLogExporter` 类型使用扩展的 HTTP 传输路径，向 transformer 交付只读记录快照，并发送它产出的**最终请求字节和请求头**；不再套用 OTLP JSON 包装或随后再次 gzip。可选 `transport` 允许调用方逐实例注入本地模拟传输；此时未传 transformer 的实例仍用官方 OTLP JSON 序列化和批次结果规则，不能因 mock 改格式。两种装配只改变单个实例的格式，不改变 filter、配置来源、持久队列或调度器。

`transformer` 是纯传输格式扩展点，不内置目标名称、事件分类、身份字段或远程配置。调用方自行实现：逐项编码或拒绝、按批构造请求、读取 HTTP 状态／头／响应体并解析批次结果。自定义传输必须在发出请求前完整生成 body；如需压缩或按最终字节签名，均由调用方在 `buildRequest` 中完成，exporter 原样发送该 body，并保留它的 headers。浏览器实现须读取有上限的响应体，不能直接复用当前只丢弃响应体的 OTLP fetch transport；Node 也不能在调用方签名后再次压缩。网络、超时和解析异常转换为请求级可重试结果，不伪造逐项 ACK。

```ts
interface EncodedHttpItem {
  eventId: string;
}
interface PermanentReject {
  kind: 'reject';
  eventId: string;
  reason: string;
}
type BatchResult =
  | {
      kind: 'items';
      items: readonly {
        eventId: string;
        status: 'accepted' | 'retryable' | 'rejected';
      }[];
      retryAfterMs?: number;
    }
  | { kind: 'retry' | 'pause'; retryAfterMs?: number };

interface HttpLogTransformer<EncodedItem extends EncodedHttpItem> {
  encode(
    record: Readonly<LogRecordSnapshot>
  ): Promise<EncodedItem | PermanentReject>;
  buildRequest(items: readonly EncodedItem[]): Promise<{
    body: Uint8Array; // 最终发送字节，之后不再修改
    headers: Readonly<Record<string, string>>;
  }>;
  parseResponse(
    response: {
      status: number;
      headers: Readonly<Record<string, string>>;
      body: Uint8Array;
    },
    items: readonly EncodedItem[]
  ): Promise<BatchResult>;
}

interface HttpTransport {
  send(request: {
    url: string;
    body: Uint8Array;
    headers: Readonly<Record<string, string>>;
    timeoutMs: number;
  }): Promise<{
    status: number;
    headers: Readonly<Record<string, string>>;
    body: Uint8Array;
  }>;
}
interface HttpLogExporterOptions<EncodedItem extends EncodedHttpItem> {
  url: string;
  transformer?: HttpLogTransformer<EncodedItem>;
  transport?: HttpTransport; // 逐实例注入；省略时使用真实 HTTP
}

new HttpLogExporter({ url: defaultUrl }); // 默认 OTLP
new HttpLogExporter({ url: customUrl, transformer: callerTransformer });
```

`EncodedHttpItem.eventId` 必须等于原 `event.id`；`PermanentReject` 只结算该记录在本注册项的投递。自定义 `BatchResult.items` 必须逐项对应本次实际发送的事件 ID；响应缺项、重复 ID 或无法解析时保留相关原项重试。请求级 `retry`／`pause` 不删除投递项；明确的服务端限流时间覆盖本目标的本地退避。重试可更新请求级时间与签名，不能修改持久快照。SDK 只校验这些通用结果，不解释响应体的业务字段。

默认 OTLP 委托的回调只有批次级 `SUCCESS`／`FAILED`，没有事件 ID 级 ACK。`SUCCESS` 仅标记整批已交付 HTTP 接收端，不能证明下游索引完成；`FAILED` 或未知结果保留整批原 ID 重试。现有 `OTLPLogExporter` 对包含 OTLP `partial_success` 的 HTTP 成功响应仍回调 `SUCCESS`，且该响应不提供逐事件 ID；因此默认路径不能声称逐项确认。需要精确逐项结果的目标须由调用方提供 transformer 和可解析的目标响应契约。

exporter 的注册项可挂多个配置来源，来源可由调用方读取环境变量、调用 fetch 接口或订阅运行时状态；transformer 不承担固定的远程配置方法。SDK 只定义以下通用契约，具体地址、鉴权与响应字段映射留在包外：

```ts
interface ExporterConfigSource {
  id: string; // 同一目标内稳定唯一，供逐来源恢复缓存
  fallback: ConfigDecision; // 首次加载失败时的显式决定
  load(): Promise<ConfigDecision>;
  subscribe?(notify: () => void): () => void; // 环境／运行时变更时主动刷新
  refreshJitterRatio?: number; // 周期拉取避免同时请求
  failureRefreshAfterMs?: number; // 不设置则沿用缓存／fallback 的刷新间隔
}

interface ConfigDecision {
  enabled: boolean;
  disabledEvents?: readonly string[];
  version?: number; // 有版本的来源只接受递增版本
  refreshAfterMs?: number; // 不设置则仅在启动或收到 notify 时读取
}
```

processor 在 `start()` 异步恢复每个 `(exporterId, targetKey, sourceId)` 的缓存，并调用来源的 `load()`：本地／mock 来源的 Promise 在启动完成前结算，远程读取在后台完成，启动期间先用该来源缓存或显式 fallback。成功时校验版本、异步持久化单来源结果、重算组合决定并唤醒目标。刷新失败只沿用该来源的最近有效结果，未有缓存时沿用其 fallback；该来源按自己的刷新／失败计划再次读取，不使用发送退避，也不影响其他来源。`version` 不高于缓存时忽略；无版本来源每次成功读取都替换该来源状态。刷新间隔由来源结果给出，processor 加该来源注册的抖动后设置下一次截止。mock 来源是业务注入的普通来源，不触发 SDK 内置特殊逻辑；宿主需用独立的来源 ID 与目标身份隔离 mock 和真实缓存／队列。

配置组合只影响注册它的目标：任一来源 `enabled=false` 就暂停该目标的新入队和旧队列发送／重试；当前在途请求仍可完成并结算 ACK，恢复后补传。任一来源将事件名列入 `disabledEvents` 时，新同名事件不入队，未在途的旧队列同名项在下次组批前移除。应用配置、`appendFanout` 和 `claim` 在存储事务中复核，避免“目标已准入、开关随后关闭、仍然入队／发送”的竞态。开关造成的抑制／移除单独计数，不算发送失败；同一 LogRecord 的其他已准入 exporter 不受影响。filter 仍只由 registry 在 processor 之前运行，配置来源不替代 filter。

## 5. npm 包边界与发布

在现有 npm workspaces 下新增独立目录 `packages/durable-logs/`，其自身 `package.json` 使用发布者拥有的 scope，例如占位名 `@your-scope/otel-durable-logs`；不得使用未经授权的 `@opentelemetry` scope。仓库根包继续保持 `private: true`，只发布这个新增工作区包。包名、版本和 scope 在实现时确定，不把示例名直接发布。

| 导出入口           | 内容与运行边界                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| `.`                | `TelemetryEmitter`、`ExporterRegistry`、`DurableDispatchProcessor`、注册接口和纯类型；不导入 Node 模块。 |
| `./web`            | IndexedDB 存储适配；可由浏览器打包器直接加载。                                                           |
| `./desktop`        | 文件存储适配；仅在桌面主进程加载，渲染进程通过宿主 IPC 调用。                                            |
| `./exporters/http` | `HttpLogExporter` 与可选 `HttpLogTransformer` 接口；默认 OTLP，自定义格式由调用方注入。                  |

每个子路径分别构建 ESM、CommonJS 和类型声明，`exports` 显式指向产物；根入口和 `./web` 的依赖图不得触达 `fs`、Electron 或主进程代码。`./exporters/http` 的默认路径依赖本仓库官方 `@opentelemetry/exporter-logs-otlp-http`；发布时声明经兼容测试的依赖版本，检查其浏览器／Node 条件导出均可用。自定义 transformer 不作为 SDK 内置子路径或硬编码目标发布。`@opentelemetry/api`、`@opentelemetry/sdk-logs` 仍为经兼容测试验证的 peer dependencies。独立包配置 `files` 白名单，仅包含 `dist`、公开 README 和 LICENSE；设置 `publishConfig.access=public`，不继承根包的私有发布属性。包内 README 给出 Web、桌面各一段注册与 emit 示例，示例仅用虚构端点和事件。

发布流水线按以下顺序执行，命令从仓库根目录运行；首次发布前由发布者确认 npm scope 归属并登录，CI 发布则配置 npm trusted publishing。版本遵循 SemVer，已有版本不能覆盖，预发布版本使用单独 dist-tag：

```sh
npm ci
npm run compile --workspace packages/durable-logs
npm run test --workspace packages/durable-logs
npm run test:browser --workspace packages/durable-logs
npm pack --workspace packages/durable-logs --dry-run
npm publish --workspace packages/durable-logs --access public
```

`npm pack --dry-run` 必须人工或 CI 核对发布清单：只包含编译产物和公开说明，无非公开 URL、文档引用、真实凭据或应用配置。发布后在空白 Web 消费者项目中执行 `npm install @your-scope/otel-durable-logs @opentelemetry/api @opentelemetry/sdk-logs`；桌面消费者使用同一公开包，分别导入 `./web`、`./desktop` 和 `./exporters/http` 子路径，确认类型解析、浏览器打包和桌面主进程加载；再用 `npm view @your-scope/otel-durable-logs version dist-tags` 核对已发布版本。仓库公开文档也应通过同一敏感词／URL 扫描，不能仅检查 tarball。

## 6. 验证用例与完成标准

1. **filter 准入：** 0、1、2 个 filter 命中、全部未初始化、部分已初始化、filter 抛错；验证零命中／无可用目标时 `Logger.emit` 和所有 processor 均未调用，部分可用时只投递可用目标。
2. **emit 与生命周期：** filter 读取处理前记录，前置 processor 改写事件名不改变已选目标；包装入口生成稳定事件 ID，直接调用原生 `Logger.emit` 不进入持久队列；验证所有公开入口与存储／exporter／配置源的 Promise 契约，`onEmit` 仅同步拍快照，入队事务提交前不返回 `queued`。模拟启动前缓冲、logger 禁用、入队失败、断网和 ACK 前崩溃，验证异步错误回执与原 ID 恢复。
3. **队列初始化：** 首次启动、并发／重复 `start()`、单个 exporter 初始化失败后重试、schema 迁移失败、完整／不完整事务重放、未知目标积压、过期／未过期租约、排空状态与逐来源配置缓存恢复；确认启动不清队列、不提前 ACK，失败目标恢复可用后自动唤醒积压。
4. **调度：** 条数和队首等待分别触发，队首已过等待时间但仍在退避时不空转；逐目标单在途、跨目标并行，按条数／字节缩批后释放多领租约，过期租约的旧 ACK 被 fencing token 拒绝；验证部分 ACK、永久拒绝、超时、服务端限流、离线恢复、`forceFlush` 和 `shutdown`。
5. **默认 HTTP 格式：** 未提供 transformer 时，用官方 OTLP exporter 的模拟传输验证浏览器／Node 的 OTLP 请求、原始 `event.id` 和时间保留、整批成功与失败重试；验证 `partial_success` 不被错误解释为逐项 ACK。
6. **自定义 HTTP 格式：** 用虚构目标的固定向量覆盖编码后最终字节、请求头、响应体、部分／畸形 ACK、网络和超时；浏览器需实际读取限长响应体，Node 需验证签名后的字节不再被压缩或修改。transformer 错误只影响其注册目标；未传 transformer 的默认路径仍发送标准 OTLP。向两个相同类型实例分别注入本地模拟 transport，确认格式、队列、结果和配置缓存独立且没有网络请求。
7. **多来源开关：** 环境、远程、运行时及宿主 mock 来源的全部组合，整体／指定事件关闭、在途 ACK、恢复、旧版本、逐来源异步拉取失败与重启缓存；mock 和真实来源 ID／目标身份隔离，任一来源关闭只影响对应 exporter，其他已准入目标照常发送。
8. **异步性能：** 慢配置来源、慢存储、慢传输和突发批量事件均不阻塞页面／桌面主线程；编码、压缩、签名在 worker 执行并限制并发，`emit()` 只在落盘完成后回 `queued`。通过事件循环延迟与队列上限测试，确认没有同步文件 I/O 或仅用 Promise 包装的主线程重计算。
9. **发布物：** `npm pack` 清单、Web／桌面子路径导入、类型解析、浏览器包的 Node 依赖扫描、空白项目安装与公开内容扫描全部通过。

OpenTelemetry 生命周期依据本仓库 `api/src/logs/Logger.ts`、`packages/sdk-logs/src/MultiLogRecordProcessor.ts` 与 `LogRecordProcessor.ts`；目标协议的具体接入材料由使用方在包外维护。
