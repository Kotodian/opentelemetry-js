# OpenTelemetry 日志投递扩展设计

状态：设计评审稿（2026-10-29）。本扩展只有**一个正在工作的投递 processor**，与 exporter 数量无关：当前阶段选用上游 `BatchLogRecordProcessor`，需要重启恢复时再将同一个装配位切换为 `DurableDispatchProcessor`。ConfigSource 顶层的独立 filter 从已声明的 exporter 中选出目标并检查初始化状态，再交给这个 processor，由它调度到对应 exporter。两种模式是互斥替换，不并列注册，也不为每个 exporter 创建 processor。基于本仓库 OpenTelemetry JS，只新增异步事件入口、注册及路由能力、可选的持久化调度 processor、带可选 `transformer` 的通用 HTTP 日志 exporter 和 Web／桌面存储适配；不修改上游 `Logger`、`LoggerProvider`、已有 processor／exporter。未传 `transformer` 时默认使用本仓库的 `@opentelemetry/exporter-logs-otlp-http` 发送 OTLP 日志；传入时由调用方定义 HTTP 请求和响应格式。SDK 不内置任何目标类型或事件分类。公开包不包含接入方的地址、凭据、事件名单、私有协议映射或内部文档；这些由调用方在包外注入。

实现进展（与目标设计区分）：`packages/durable-logs/` 已实现单个 `DurableDispatchProcessor`、注册表、异步入口、HTTP exporter 和 Web／桌面存储适配；桌面接入当前也使用 Durable。**Batch 模式的单 processor 路由与回执适配尚未实现**，不能仅替换构造函数就声称已切换。本文只修改设计，不代表 SDK 或业务代码已切换。浏览器运行、主线程延迟及 worker 卸载仍按第 6 节验收。

**现有源码边界：** `api/src/logs/Logger.ts` 的 `emit(): void`、`packages/sdk-logs/src/LogRecordProcessor.ts` 的 `onEmit(): void`、`packages/sdk-logs/src/export/LogRecordExporter.ts` 的回调式 `export(..., callback): void` 均保持原样。`LoggerProvider` 已有 `resource`／`processors` 装配点；`experimental/packages/exporter-logs-otlp-http` 已提供浏览器和 Node 的 `OTLPLogExporter`，但构造函数固定使用 `JsonLogsSerializer`，配置仅能改变 URL、头等传输参数，无法改变请求体格式。底层 `ISerializer` 可替换字节序列化，但现有浏览器传输丢弃响应体，现有成功回调也不表达逐项结果；自定义协议因此需要新增通用 HTTP 扩展，不能只改现有 exporter 配置。上述扩展目前实现在独立工作区包中，原有 OTel API 与处理器未改。`BatchLogRecordProcessorBase.ts` 的现有默认值仅是批次 512、内存队列 2048、等待 1000 ms、导出超时 30000 ms；不能把这四项说成持久化重试或 HTTP 调度的默认值。

## 1. 对外只接收 ConfigSource

业务初始化 SDK 时**只提供一个 `TelemetryConfigSource` 对象**。对象直接包含 `exporters[]`、一个顶层 `filter`、一个 `processor`，以及可选 resource。每个 exporter 条目配置其已注册类型、URL、可选 transformer、运行时配置来源和 Durable 专用投递策略；顶层 filter 返回命中的 exporter ID 列表，可为一个、多个或空列表。业务不再传第二份初始化对象，也不调用 `registry.register()`。本地环境数据可在创建对象前异步读取；各 exporter 的配置来源可异步加载远程／mock 开关，SDK 初始化与投递仍是异步的。远程 JSON 只提供数据；函数和组件类型由调用方在本地代码选定，不能从远程执行代码。

`TelemetryConfigSource` 是依赖注入的唯一根：SDK 按 `type` 解析已注册的 processor/exporter 工厂，再用同一对象的参数在内部创建和接线；不会把 Batch 的底层出口依赖暴露给业务。OpenTelemetry 仍按现有机制在 `LoggerProvider.processors` 注册；装配始终是 `processors: [processor]`，与 exporter 数量无关。切换 Batch／Durable 只改 ConfigSource 的**一个** processor 类型和选项，不改 exporter 清单或业务调用入口。filter 与 processor 分离：`TelemetryEmitter` 在调用 OTel `Logger.emit` **之前**执行一次顶层 filter，只取其返回且已声明、已初始化的目标；processor 只处理已选定目标。没有命中或命中目标全未初始化时，不调用 `Logger.emit`。没有可注册 exporter 时返回异步 no-op `emit`，不生成事件 ID、不创建 processor 或队列。

```ts
interface TelemetryConfigSource {
  resource?: Resource;
  exporters: readonly {
    id: string;                // filter 返回这个 ID，实例由 SDK 创建
    targetKey: string;         // 目标身份，用于 Durable 队列隔离
    type: ExporterType;        // 已注册工厂的类型标识，不是实例
    url?: string;
    transformer?: HttpLogTransformer;
    transport?: HttpTransport;
    configSources?: readonly ExporterConfigSource[];
    deliveryPolicy?: DeliveryPolicy; // 仅 Durable 使用
  }[];
  filter(record: Readonly<PreparedLogRecord>): Promise<readonly string[]>;
  processor: { type: ProcessorType; options?: ProcessorOptions };
}

const settings = await readLocalSettings();
const configSource: TelemetryConfigSource = {
  resource: settings.resource,
  exporters: [
    { id: 'collector', targetKey: settings.collector.targetKey,
      type: 'http', url: settings.collector.url }, // 默认 OTLP
    { id: 'external-platform', targetKey: settings.external.targetKey,
      type: 'http', url: settings.external.url,
      transformer: settings.external.transformer,
      configSources: settings.external.configSources }, // 自定义 HTTP 格式
  ],
  filter: async record => selectExporterIds(record), // 返回 []／['collector']／['external-platform']／两者
  processor: { type: 'batch', options: settings.batch },
};

// 目标 API：唯一初始化参数是 configSource；其他组件由 SDK 内部装配。
const telemetry = await createTelemetry(configSource);
await telemetry.emit(event);
```

`ExporterType`／`ProcessorType` 是已注册工厂的类型标识，`'http'`／`'batch'` 为此设计中的注册示例，**不是现有包已发布的 API**。调用方可增加其他注册类型，SDK 不内置目标名称或业务筛选规则。示例 `exporters` 中是**两个独立的目标**，分别由同一个 HTTP 工厂创建实例：默认 OTLP 发 Collector，自定义 transformer 发外部平台；缺一方的 URL／必要参数时只跳过该目标。顶层 filter 返回目标 ID，不直接返回尚未初始化的 exporter 实例；既可只选一个，也可同时选中两个。上游 Batch 构造函数只接收一个 `LogRecordExporter`，SDK 须在内部处理向多个已选目标分发，具体接线不出现在 ConfigSource 对象或业务调用中。

`createTelemetry(configSource)` 是公开接口，上面的定义是装配契约。SDK 内部仅做通用装配：校验对象中的注册类型和参数、并发创建配置齐全的 exporter、只注册成功创建的目标、初始化注册表、按 `processor.type` 创建**一次** processor、构造 LoggerProvider 和 emitter。某个目标缺 URL／必需凭据时只跳过它，不影响其他目标；若没有注册成功的目标，连 processor 都不创建。调用方只提交 ConfigSource，**不会在 SDK 初始化处传入 exporter 实例**。SDK 不内置目标名称、URL 或格式分支。

每个 exporter 的 `configSources` 可独立订阅运行时开关；processor／顶层 filter／transformer／目标身份等**结构变化**则由业务提供新的 ConfigSource 对象，SDK 有序关闭旧运行实例后重新初始化。不能在旧 processor 的队列仍关联旧 `targetKey` 时把它接到新 exporter。注册前必需参数校验与注册后来源开关是两件事，不能用远程开关代替地址／凭据校验。SDK 不定义业务事件类型或固定属性名；同一事件被 filter 选中多个可用目标时，经过同一个 processor 再按目标分别投递。`transformer` 只定义单个 HTTP 目标的传输格式，不决定 filter 命中。

```ts
interface ExporterRegistration {
  id: string; // 同一 ExporterRegistry 内稳定且唯一
  targetKey: string; // 环境 + 目标身份，重启后仍可辨认
  exporter: DurableLogExporter;
  deliveryPolicy: DeliveryPolicy; // 当前接口必填；仅 Durable 使用逐目标持久化策略
  configSources?: readonly ExporterConfigSource[]; // 从 ConfigSource.exporters[] 解析
}

// 这是目标中的内部注册形态；当前实现仍在注册项中持有 filter，迁移时要移至注册表顶层。
```

`TelemetryConfigSource.exporters[]` 的每个目标可内含多个 `ExporterConfigSource`，用于该目标的运行时开关。它们是顶层对象的字段，**不是额外的 SDK 初始化参数**。每个来源有稳定 `id`、显式 `fallback`、异步 `load()` 和可选变更订阅。多个来源时 `enabled` 取逻辑与、`disabledEvents` 取并集；没有来源的目标默认可投递。配置来源与 `transformer` 相互独立，配置状态不改变 exporter 实例和旧队列的身份。

定义可以产出任意多个 exporter，甚至多个同类型 HTTP 目标，但**不会增加 processor 实例**。Durable 模式按注册项 `id + targetKey` 隔离持久队列、配置缓存、调度状态和 ACK；同一 LogRecord 命中几个可用目标，就由唯一 processor 原子写入几个投递项。Batch 模式只有唯一内存批处理队列，路由出口按所选目标拆分批次；既不提供逐目标持久队列，也不借用 `deliveryPolicy` 假装有重启恢复。现有 `ExporterRegistration.deliveryPolicy` 仍必填；目标契约将其改为仅 Durable 必需，Batch 使用单个 processor 的批处理选项，不让 ConfigSource 伪造无效策略。两种模式都不假设固定目标组合。

`TelemetryEmitter.emit()` 只执行 ConfigSource 顶层的一个异步、只读、无副作用 filter，得到 exporter ID 列表；`ExporterRegistry` 验证这些 ID 已在当前对象声明并只准入已初始化的目标。filter 返回 `Promise<readonly string[]>`，不得修改记录或发网络请求；未知／重复 ID 拒绝本次准入，不能默默投到别的目标。空列表返回 `unmatched`；选中的目标都未初始化时返回 `unavailable`。两种情况都不调用 `Logger.emit`，也不让 processor 处理。若只有部分目标已初始化，仅这些目标进入后续处理。filter 拒绝或超时，本次准入失败，包装入口 reject，仍不调用 `Logger.emit`。**当前 `ExporterRegistry.register()` 仍要求逐目标 filter，`select()` 也逐项调用，必须在实现此设计时迁移；本文不声称已完成。**

注册只允许在 `ExporterRegistry.initialize()` 前完成；重复 exporter ID、缺顶层 filter 或无目标身份直接拒绝初始化。`initialize()` 对各 exporter 独立等待并返回逐目标结果：成功的实例标记为可用，单个失败不阻止其他目标启动；失败的实例可单独重试初始化，期间不会被新事件准入。registry 在目标初始化成功、失效或重新初始化时通知 processor 唤醒该目标的已有积压。`resolveTarget(id, targetKey)` 必须同时核对目标身份，不能把旧环境的队列交给同 ID 的新目标。存储不可用则由 `DurableDispatchProcessor.start()` 报错。`targetKey` 包含环境与目标身份，队列按 `exporterId + targetKey` 隔离；宿主换环境后，旧目标的积压保持待处理，不会用新凭据发送，须由同一目标重新注册或显式迁移／丢弃。Web 包只加载 IndexedDB 适配，桌面包只加载主进程文件适配，不把 Node／Electron 依赖带进 Web。

## 2. emit：统一的 LogRecord 输入

调用方的事件规则决定触发时机、事件名、属性和 body；SDK 不定义分类标签。`TelemetryEmitter` 补事件 ID 与发生时间，形成不可变的 `PreparedLogRecord`，执行 ConfigSource 的顶层 filter 选目标，再由 `ExporterRegistry` 核对这些目标的初始化状态；只有存在可用目标时才调用原生 `Logger.emit`。这里处理的是日志事件，不创建另一套 OTel Meter 指标管线。emit 不指定 exporter，同一事件也不因可能去两个目标而创建两种形状。

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

`TelemetryEmitter.emit(): Promise<EmitReceipt>` 是有目标时的统一入口：它在准入前生成非空 `event.id`，顶层 filter 读取的是此时的 `PreparedLogRecord`。原生 `Logger.emit` 和 `LogRecordProcessor.onEmit` 均返回 `void`；直接调用原生 `Logger.emit` 不参与目标选择。有目标但 filter 返回空列表时返回 `unmatched`；选中的目标全未初始化时返回 `unavailable`。**注册表为空**与这两种有目标的结果不同：外层装配直接提供异步 no-op `emit`，不实例化 `TelemetryEmitter`，不生成事件或 `EmitReceipt`。

**异步边界。** 调用方异步提交事件，不等待网络 ACK，也不让埋点失败改变原操作结果。SDK 新增的 `initialize()`、`start()`（仅 Durable）、`emit()`、filter、`forceFlush()`、`shutdown()`、配置 `load()`、存储读写／落盘（仅 Durable）、transformer 的编码／请求构造／响应解析和 HTTP 发送均返回 Promise。上游 OTel 的 `Logger.emit()`／`onEmit()` 签名仍是同步 `void`，无法改变；在异步 filter 完成后，这两个同步桥接只传递投递计划和必要的有界记录，不做文件 I/O、加密／压缩或网络请求。耗时的序列化、压缩和加密不能仅包装进 Promise 后继续占用 UI／Electron 主线程；实现须采用浏览器 Worker 或桌面 worker 执行，并限制单批大小和并发。Batch 的异步回执只表明已调用唯一的内存 processor，**不保证队列收下、发送成功或重启恢复**；Durable 在异步存储事务提交后才回 `queued`，失败 reject。启动前事件由调用方有界缓冲并保留原发生时间；缓冲事件不得提前宣称 `submitted` 或 `queued`，初始化失败须报告丢弃。这里描述的是拟实现的目标状态，不代表当前仓库已完成 Batch 适配或所有 worker 卸载。

准入成功后，emitter 确定命中且已初始化的目标 ID、`targetKey` 与事件 ID，再调用 `Logger.emit`。Durable 使用现有实现中的 OTel `Context` 向 `DurableDispatchProcessor.onEmit(logRecord, context)` 传递目标列表并拍最终快照。**上游 Batch 不会把 `onEmit` 的 Context 随记录保留到 `export()`**，因此 Batch 模式必须在 SDK 内将已选目标编码到保留的、可随 LogRecord 入队的路由元数据；唯一的路由 exporter 在发送前读取并移除该元数据，不能让它进入任何目标的业务数据，不能在导出时重跑 filter。事件 ID 与路由元数据均为 SDK 保留字段，调用方不可覆盖。两种模式都不得让 processor 执行 filter。

```ts
// 设计目标：Batch 需扩展现有回执类型；当前包尚无 submitted 状态。
interface EmitReceipt {
  eventId: string;
  matched: string[]; // filter 命中的 exporter ID
  unavailable: string[]; // 命中但尚未初始化的 exporter ID
  submitted: string[]; // Batch：已调用内存 processor；是否收下未知
  queued: string[]; // Durable：已持久化的目标；网络尚未确认
  suppressed: Array<{ exporterId: string; reason: string }>; // 仅该目标的组合配置
  status: 'submitted' | 'queued' | 'suppressed' | 'unmatched' | 'unavailable';
}
```

零 filter 命中返回 `unmatched`；有命中但无已初始化 exporter 返回 `unavailable`。这两种回执都在调用 `Logger.emit` 前完成，`submitted` 和 `queued` 均为空。Batch 只报告 `submitted`，绝不能报告 `queued` 或承诺成功；其容量溢出由上游 processor 自行丢弃并记录诊断。Durable 的 `onEmit` 同步拍快照，异步把所有允许投递的目标在同一存储事务中入队；提交后才回 `queued`，失败 reject，不能出现部分入队却返回成功。全部可用目标被组合配置开关抑制时返回 `suppressed`。logger 被禁用或已调用 `Logger.emit` 却未进入选定投递 processor 时，不能虚报成功。**当前 `TelemetryEmitter` 强制检查 `plan.claimed` 并点名 Durable，故 Batch 模式必须先改这一处 SDK 回执契约；单纯注入原生 Batch 必然报错。**

## 3. 单个 processor：Batch 当前，Durable 后续

**互斥装配。** `LoggerProvider.processors` 只装一个投递 processor；注册了一个、两个或更多 exporter 都不改变这个数量。当前阶段 ConfigSource 的唯一 processor 定义创建上游 `BatchLogRecordProcessor`，其构造函数只接受**一个** `LogRecordExporter`，因此 SDK 向该定义注入**一个路由 exporter 适配器**作为出口：适配器从每条记录的 SDK 路由元数据取已选目标，按目标分组，分别调用对应的异步 `HttpLogExporter`，在发给目标之前移除路由元数据。适配器是 exporter，**不是第二个 processor**，更不能按目标构造若干个 Batch processor。不同目标的调用可异步并行，适配器按上游回调契约汇总完成结果；上游 Batch 对失败批次不会提供持久重试，因此失败不能在回执中伪称已投递。Batch 配置只设置这一实例的 `maxQueueSize`、`maxExportBatchSize`、`scheduledDelayMillis`、`exportTimeoutMillis`；没有逐目标持久队列、跨重启恢复、原子 fan-out 和 Durable 的逐项 ACK 保证。

```text
事件 → ConfigSource.filter 选目标 → ExporterRegistry 查初始化状态 → 一个 BatchLogRecordProcessor
     → 一个路由 exporter 适配器 → 已选中的一个或多个 HttpLogExporter
未来切换：同一装配位换为一个 DurableDispatchProcessor → 已选中的 exporter
```

Batch 模式的配置来源仍挂在各 exporter 注册项，而非挂在 processor 上。当前配置来源的读取、组合和缓存由 Durable 实现负责；要让 Batch 模式遵守同一开关，应将这部分通用状态管理移到注册表／独立配置服务，并让 Batch 路由适配器在发送前核对当前决定。Batch 的内存缓冲不能重启恢复，被禁用的在途事件按该模式的尽力而为语义处理。不同来源的失败与开关只影响其注册目标。没有配置来源的目标默认启用。**这一配置状态的复用也是切换 Batch 的 SDK 前置工作，当前代码尚未完成。**

### Durable：持久化队列与调度（保留的切换目标）

切换后 `DurableDispatchProcessor` 是唯一的投递 processor，不与 Batch 并存。它只接收 emitter 已准入的目标列表，不参与 filter 定义或匹配。`onEmit(ReadWriteLogRecord, Context)` 在当前调用栈中读取投递计划，并复制 `eventName`、`hrTime`、`hrTimeObserved`、`attributes`、`body`、`resource.attributes`、`instrumentationScope`、severity 等需要异步使用的值；上游源码允许在 onEmit 期间修改记录，但回调返回后异步处理只能读取，因此**不能把可变 LogRecord 对象直接留在队列里**。快照序列化时拒绝无法持久化的值，保留 `event.id` 作为所有目标的稳定去重键。

```text
TelemetryEmitter → ConfigSource.filter 选目标 → ExporterRegistry 核对初始化状态
  → 无可用 exporter：返回，不调用 Logger.emit，也不进入 processor
  → 有可用 exporter：把目标列表放入 Context，再调用 Logger.emit
  → 唯一投递 processor：DurableDispatchProcessor.onEmit
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

实现提供可选的 retryDelaysMs 固定失败阶梯、retryJitterMode 对称抖动、
queueEviction 高水位和淘汰比例；这些都按注册目标独立生效。不配置时保留
既有指数退避与硬容量限制。分区持久保存连续失败次数，收到有效的服务端
限流时间时精确采用该时间且不推进本地失败阶梯；部分 ACK 仍有可重试项时
阶梯推进，全 rejected 不重置，完整成功后归零。被租约保护的在途项不参与
容量淘汰。可选 DeliveryPolicySource 通过异步 load 和变更订阅提供运行时
策略覆盖，processor 从下一次调度起使用更新后的阈值。

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

`forceFlush()` 绕过条数／队首等待门槛并唤醒目标，但不绕过组合开关、exporter 未初始化、离线、服务端限流或已持久化的退避截止；其 `pendingForce` 保留到本轮发送或超时，不会因最短间隔到期前的定时等待丢失。它等待已入队记录收到终态 ACK；无法继续发送时明确失败，超过 `ForceFlushOptions.timeoutMillis`（默认 30 秒）时超时失败，不把仍在队列里的记录报告为完成。`shutdown()` 先停止新的入队并尝试同等的 flush，随后停止定时器、等待进行中的 append 落盘并关闭存储；失败或超时的未确认项保留到下次 `start()` 恢复。

## 4. exporter 的输入、结果和远程配置

自定义 HTTP 路径可用 maxRequestBytes 限制编码后的请求体。超限或 HTTP
413 时递归缩批；单条仍超限则只拒绝该条，其他条继续投递。调用方的
transformer 也可抛 HttpRequestTooLargeError 表示可缩批的大小错误；其他
编码与解析错误仍保留原项重试。requestIntervalMs 可由调用方给出数值或
异步函数，逐次约束自定义请求的发送间隔，包含缩批产生的子请求；弱网
等环境分类及具体间隔仍由调用方决定。

ConfigSource 顶层的 `filter` 在 processor 之前独立执行；exporter 的编码、发送、解码接口都返回 Promise，processor 在目标的异步调度循环中等待结果。exporter 不删队列、不启动自己的重试时钟，也不使用同步加密、压缩或文件 API。发送契约返回每个投递项的 `accepted`／`retryable`／`rejected`，或请求级 `retry`／`pause`；响应项数或 ID 不对应整批输入时视为未知，processor 保留原项。发送成功但本地 ACK 事务前崩溃可能重发，这是至少一次投递，不宣称网络恰好一次。

**唯一的 `HttpLogExporter` 类型。** 这是拟新增的 Web／Node 通用扩展；ConfigSource 中的每个 `type: 'http'` 条目分别让 SDK 内部注册工厂创建一个实例，配置其 `url`、标准 HTTP 选项和**可选** `transformer`、`transport`。未传 `transformer` 时，把持久快照恢复为 `ReadableLogRecord`，默认复用本仓库已有的 `@opentelemetry/exporter-logs-otlp-http` `OTLPLogExporter`，沿用 OTLP JSON、默认 `Content-Type` 和标准 HTTP 行为；URL 从该条目取得。传入 `transformer` 时，同一 HTTP 类型使用扩展的传输路径，向 transformer 交付只读记录快照，并发送它产出的**最终请求字节和请求头**；不再套用 OTLP JSON 包装或随后再次 gzip。可选 `transport` 逐条目提供本地模拟传输；此时未传 transformer 的实例仍用官方 OTLP JSON 序列化和批次结果规则，不能因 mock 改格式。两种装配只改变单个实例的格式，不改变顶层 filter、配置来源、持久队列或调度器。

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

// 一个 ConfigSource 中的两个 HTTP 条目；SDK 分别创建两个实例。
const exporters = [
  { id: 'default', type: 'http', url: defaultUrl },
  { id: 'custom', type: 'http', url: customUrl, transformer: callerTransformer },
];
```

`EncodedHttpItem.eventId` 必须等于原 `event.id`；`PermanentReject` 只针对该记录在本注册项的投递。自定义 `BatchResult.items` 必须逐项对应本次实际发送的事件 ID。**以下保留原项重试、退避和逐项结算仅在 Durable 模式成立**：响应缺项、重复 ID 或无法解析时保留相关原项；请求级 `retry`／`pause` 不删除投递项；明确的服务端限流时间覆盖本目标的本地退避。重试可更新请求级时间与签名，不能修改持久快照。Batch 模式的路由出口可解析同样的 HTTP 结果，但上游内存 processor 不负责按事件 ID 重试，失败结果只能向回调及诊断报告。SDK 只校验这些通用结果，不解释响应体的业务字段。

默认 OTLP 委托的回调只有批次级 `SUCCESS`／`FAILED`，没有事件 ID 级 ACK。`SUCCESS` 仅标记整批已交付 HTTP 接收端，不能证明下游索引完成；**Durable** 收到 `FAILED` 或未知结果时保留整批原 ID 重试，**Batch** 只能报告失败，不能承诺重试。现有 `OTLPLogExporter` 对包含 OTLP `partial_success` 的 HTTP 成功响应仍回调 `SUCCESS`，且该响应不提供逐事件 ID；因此默认路径不能声称逐项确认。需要精确逐项结果的目标须由调用方提供 transformer 和可解析的目标响应契约。

顶层 `TelemetryConfigSource` 的目标定义可内含多个运行时配置来源；它们可读取环境变量、调用 fetch 接口或订阅运行时状态，**不作为 `createTelemetry` 的其他参数**。transformer 不承担固定的远程配置方法。SDK 只定义以下通用契约，具体地址、鉴权与响应字段映射由顶层 ConfigSource 在包外决定：

```ts
interface ExporterConfigSource {
  id: string; // 同一目标内稳定唯一，供逐来源恢复缓存
  fallback: ConfigDecision; // 首次加载失败时的显式决定
  load(): Promise<ConfigDecision>;
  initialLoad?: 'blocking' | 'background'; // 本地/mock 可阻塞启动；默认远程后台刷新
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

配置状态在设计目标中归注册表／共享配置服务管理，供 Batch 路由出口和 Durable processor 读取；**不是每个 exporter 各有一个 processor**。目前源码由 Durable 在 `start()` 恢复每个 `(exporterId, targetKey, sourceId)` 的缓存并调用来源的 `load()`；切到 Batch 前需抽出这段共享生命周期。Batch 的配置状态只保留在内存，Durable 可通过其存储适配恢复并持久化逐来源缓存。本地／mock 来源的 Promise 在启动完成前结算，远程读取在后台完成，启动期间先用该来源缓存或显式 fallback。成功时校验版本、更新单来源结果、重算组合决定并唤醒相关目标。刷新失败只沿用该来源的最近有效结果，未有缓存时沿用其 fallback；该来源按自己的刷新／失败计划再次读取，不使用发送退避，也不影响其他来源。`version` 不高于缓存时忽略；无版本来源每次成功读取都替换该来源状态。mock 来源是业务注入的普通来源，不触发 SDK 内置特殊逻辑；宿主需用独立的来源 ID 与目标身份隔离 mock 和真实缓存／队列。

配置组合只影响注册它的目标：任一来源 `enabled=false` 就禁止该目标新投递；Durable 的旧队列暂停发送／重试，当前在途请求仍可结算 ACK，恢复后补传；Batch 的内存缓冲在发送前重检开关，无法重启补传。任一来源将事件名列入 `disabledEvents` 时，新同名事件不投递；Durable 未在途的旧队列同名项在下次组批前移除，Batch 中尚未导出的记录在路由时跳过该目标。Durable 的 `appendFanout` 和 `claim` 在存储事务中复核配置；Batch 在路由发送前复核，但没有持久事务保证。开关造成的抑制／移除单独计数，不算发送失败；同一 LogRecord 的其他已准入 exporter 不受影响。filter 仍只由 registry 在 processor 之前运行，配置来源不替代 filter。

## 5. npm 包边界与发布

现有 npm workspace `packages/durable-logs/` 的包名是通用的 `opentelemetry-sdk`，本地 `package.json` 当前版本为 `3.0.1`；本次设计文档不变更包名或版本，不使用未经授权的 `@opentelemetry` scope。仓库根包继续保持 `private: true`。

| 导出入口           | 内容与运行边界                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| `.`                | 目标公开单入口 `createTelemetry(configSource)`；现有 `TelemetryEmitter`、`ExporterRegistry`、`DurableDispatchProcessor` 和纯类型仍可保留供扩展，但业务不需逐项装配。Batch 路由能力待新增，不导入 Node 模块。 |
| `./web`            | IndexedDB 存储适配；可由浏览器打包器直接加载。                                                           |
| `./desktop`        | 文件存储适配；仅在桌面主进程加载，渲染进程通过宿主 IPC 调用。                                            |
| `./exporters/http` | `HttpLogExporter` 与可选 `HttpLogTransformer` 接口；默认 OTLP，自定义格式由 ConfigSource 的目标条目配置。 |

每个子路径分别构建 ESM、CommonJS 和类型声明，`exports` 显式指向产物；根入口和 `./web` 的依赖图不得触达 `fs`、Electron 或主进程代码。`./exporters/http` 的默认路径依赖本仓库官方 `@opentelemetry/exporter-logs-otlp-http`；发布时声明经兼容测试的依赖版本，检查其浏览器／Node 条件导出均可用。自定义 transformer 不作为 SDK 内置子路径或硬编码目标发布。`@opentelemetry/api`、`@opentelemetry/sdk-logs` 仍为经兼容测试验证的 peer dependencies。独立包配置 `files` 白名单，仅包含 `dist`、公开 README 和 LICENSE；设置 `publishConfig.access=public`，不继承根包的私有发布属性。包内 README 给出 Web、桌面各一段只传 `configSource` 的初始化与 emit 示例，示例仅用虚构端点和事件，不让业务手工注册 exporter 或 processor。

验证由仓库现有 Nx 与 CI 流水线执行：根目录编译、单测、浏览器测试、lint，随后由 `verify-pack` 作发布物入口与类型声明检查；不用开发者单独执行 `npm pack`。后续代码实现需在通过评审与验证后按仓库现有 release PR 和 `publish-to-npm` 工作流发布；本设计不擅自指定新的版本号：

```sh
npm ci --ignore-scripts
npm run compile
npm run test
npm run test:browser
npm run lint
npm run verify-pack
```

真实浏览器测试已接入 `test:browser`。worker 性能验收仍需在后续 Batch 适配发布前完成；上面的命令说明仓库流水线，不代表本文设计的 Batch 能力已经发布。

`verify-pack` 须核对发布清单：只包含编译产物和公开说明，无非公开 URL、文档引用、真实凭据或应用配置。发布后在空白 Web 消费者项目中安装 `opentelemetry-sdk`、`@opentelemetry/api` 和 `@opentelemetry/sdk-logs`；桌面消费者使用同一公开包，分别导入 `./web`、`./desktop` 和 `./exporters/http` 子路径，确认类型解析、浏览器打包和桌面主进程加载；再核对已发布版本与 dist-tag。仓库公开文档也应通过同一敏感词／URL 扫描，不能仅检查 tarball。

## 6. 验证用例与完成标准

1. **单一 ConfigSource 与准入：** 仅通过 `createTelemetry(configSource)` 注入资源、一个 processor 配置、0／1／2 个 exporter 条目及各自多个配置来源，验证 SDK 内部按已注册类型自动装配。单目标缺地址／必需凭据时只跳过该目标；零目标 `emit` 空操作且不创建 processor；切换 Batch／Durable 只改变 ConfigSource 的一个 processor 类型。再验证顶层 Filter 返回 0／1／2 个目标 ID、未知／重复 ID、目标未初始化或部分初始化、Filter 抛错；确认 `LoggerProvider.processors` 始终至多只有一个投递 processor，未命中／不可用时不调用它。
2. **Batch（当前阶段）：** 上游只有一个 `BatchLogRecordProcessor` 和一个路由 exporter 适配器；验证两个命中目标共享这一内存队列，按路由分别投递，过滤发生在入队之前且导出时不重跑，私有路由元数据不出网。内存满、导出失败、关闭及重启时不返回 Durable 的 `queued`／补传语义；修改 `TelemetryEmitter` 的回执契约后才允许切换，不能留下 `plan.claimed` 运行时错误。多来源开关关闭时该目标不发，其他目标不受影响。
3. **Durable（后续切换）：** filter 读取处理前记录，包装入口生成稳定事件 ID，直接调用原生 `Logger.emit` 不进入持久队列；验证公开入口与存储／exporter／配置源的 Promise 契约，`onEmit` 仅同步拍快照，入队事务提交前不返回 `queued`。模拟启动前缓冲、logger 禁用、入队失败、断网和 ACK 前崩溃，验证异步错误回执与原 ID 恢复。
4. **Durable 队列与调度：** 首次启动、并发／重复 `start()`、单个 exporter 初始化失败后重试、schema 迁移失败、事务重放、未知目标积压、租约、逐来源缓存恢复；验证条数／队首等待、单目标单在途、跨目标并行、缩批、部分 ACK、退避、离线、`forceFlush` 和 `shutdown`，确认一个 processor 管理所有目标分区。
5. **默认 HTTP 格式：** 未提供 transformer 时，用官方 OTLP exporter 的模拟传输验证浏览器／Node 的 OTLP 请求、原始 `event.id` 和时间保留、整批成功与失败报告（仅 Durable 重试）；验证 `partial_success` 不被错误解释为逐项 ACK。
6. **自定义 HTTP 格式：** 用虚构目标的固定向量覆盖编码后最终字节、请求头、响应体、部分／畸形 ACK、网络和超时；浏览器需实际读取限长响应体，Node 需验证签名后的字节不再被压缩或修改。transformer 错误只影响其注册目标；未传 transformer 的默认路径仍发送标准 OTLP。向两个相同类型实例分别注入本地模拟 transport，确认格式和结果独立，Durable 队列／缓存分区独立，且没有网络请求。
7. **多来源开关：** 环境、远程、运行时及宿主 mock 来源的组合，整体／指定事件关闭、旧版本与异步拉取失败；分别验证 Batch 的内存状态以及 Durable 的在途 ACK、恢复与重启缓存。mock 和真实来源 ID／目标身份隔离，任一来源关闭只影响对应 exporter。
8. **异步性能：** 慢配置来源、慢存储、慢传输和突发批量事件均不阻塞页面／桌面主线程；编码、压缩、签名在 worker 执行并限制并发。Batch 只回 `submitted`，Durable 只在落盘完成后回 `queued`；通过事件循环延迟与队列上限测试确认没有同步文件 I/O 或主线程重计算。
9. **发布物：** `npm pack` 清单、Web／桌面子路径导入、类型解析、浏览器包的 Node 依赖扫描、空白项目安装与公开内容扫描全部通过。

OpenTelemetry 生命周期依据本仓库 `api/src/logs/Logger.ts`、`packages/sdk-logs/src/export/BatchLogRecordProcessorBase.ts`、`packages/sdk-logs/src/export/LogRecordExporter.ts` 与 `packages/sdk-logs/src/LogRecordProcessor.ts`；目标协议的具体接入材料由使用方在包外维护。
