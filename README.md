# 高性能流式数据处理引擎

一个基于 Java 21 的轻量级高性能流式数据处理引擎，借鉴 Kafka（日志结构化存储）、Flink（事件时间 / 窗口 / Watermark）和 ClickHouse（MergeTree 增量聚合）的核心设计思想。

**已封装为 Web 演示应用！** 访问 http://localhost:8081 即可体验交互式流处理演示。

## 项目结构

```
streaming-data-engine/
├── engine/          # 核心引擎模块（LSM-Tree、窗口、AI控制）
└── web/             # Web 演示应用（Spring Boot + 前端界面）
```

## 技术栈

- **Java 21** — 虚拟线程、Records、模式匹配
- **Maven** — 构建工具（`./mvnw`）
- **Spring Boot 3.5+** — Web 启动器和 REST 演示接口
- **Lombok** — `@Slf4j`、`@RequiredArgsConstructor`、`@Getter`

## 快速开始

### 方式一：Web 演示应用（推荐）

启动交互式 Web 界面，在浏览器中体验流处理功能：

```bash
# 1. 编译整个项目
./mvnw clean install -DskipTests

# 2. 启动 Web 应用
./mvnw spring-boot:run -pl web

# 3. 打开浏览器访问 http://localhost:8081
```

Web 界面功能：
- **基础流处理演示** - filter + map 操作演示
- **聚合统计演示** - 实时数据统计分析
- **IoT 传感器模拟** - 多传感器数据实时聚合
- **自定义数据处理** - 提交 JSON 数据实时处理
- **引擎状态监控** - 实时查看内存和性能指标

### 方式二：核心引擎测试

```bash
# 编译
./mvnw compile

# 运行全部测试
./mvnw test

# 运行单个测试类
./mvnw test -Dtest=BackpressureControllerTest

# 运行单个测试方法
./mvnw test -Dtest=IoTScenarioTest#testSensorDataAggregation
```

## Web API 接口

启动 Web 应用后，可以通过以下 REST API 调用流处理功能：

### 演示接口

| 接口 | 方法 | 描述 |
|-----|------|------|
| `GET /` | - | Web 界面主页 |
| `GET /api/stream/status` | - | 获取引擎状态 |
| `GET /api/stream/demo/basic` | - | 基础流处理演示（filter + map） |
| `GET /api/stream/demo/aggregate/{count}` | - | 聚合统计演示 |
| `GET /api/stream/demo/iot?sensors=3&readings=50` | - | IoT 传感器数据模拟 |

### 数据处理接口

| 接口 | 方法 | 描述 |
|-----|------|------|
| `POST /api/stream/process` | JSON | 自定义数据处理 |

**请求示例：**
```bash
curl -X POST http://localhost:8081/api/stream/process \
  -H "Content-Type: application/json" \
  -d '[{"name": "item1", "value": 10}, {"name": "item2", "value": 20}]'
```

**响应示例：**
```json
{
  "inputCount": 2,
  "outputCount": 2,
  "durationMs": 45,
  "throughput": "44.44 records/s"
}
```

## 核心特性

### 1. 存储层 - LSM-Tree
借鉴 Kafka 的 Log-Structured Merge-Tree 设计：
- **MemTable**: 内存有序结构（`ConcurrentSkipListMap`）
- **Immutable MemTable**: 等待刷盘的只读表
- **Segment**: 磁盘不可变数据段，支持稀疏索引
- **Compaction**: 后台合并压缩（Size-Tiered / Leveled）
- **WAL**: 预写日志保证数据可靠性

### 2. 窗口与事件时间处理
借鉴 Flink 的 Watermark 机制：
- **Event Time**: 基于业务时间戳处理
- **Watermark**: 推进事件时间，处理乱序数据
- **Window**: 支持滚动、滑动、会话、全局、计数窗口
- **Trigger**: 灵活的窗口触发策略
- **Allowed Lateness**: 允许迟到数据处理

### 3. 增量聚合引擎
借鉴 ClickHouse 的 MergeTree 设计：
- **分区存储**: 按分区键组织数据
- **主键排序**: 高效的范围查询
- **增量聚合**: 预聚合减少计算量
- **后台合并**: 自动合并优化存储

### 4. AI 智能控制层

#### 自适应窗口管理 (`AdaptiveWindowManager`)
- 基于 **Little's Law (L = λW) + PID 控制器** 预测并平滑调整最优窗口大小
- 使用 EWMA 实时估计数据到达率，避免固定窗口盲区
- 动态调整乱序容忍度 (`maxOutOfOrderness`)

#### 异常流量检测 (`AnomalyDetector`)
- 3-sigma 统计异常检测
- 变化率监控
- 周期性模式识别
- 多级告警机制（NORMAL / MEDIUM / HIGH / CRITICAL）

#### 动态背压控制 (`BackpressureController`)
- **Little's Law 预测式限流**: `optimalRate = arrivalRate × targetLatency / avgLatency`
- **EWMA 到达率估计**: 逐样本即时更新，无需等待固定窗口结束
- **PID 控制器平滑修正**: 比例-积分-微分反馈避免剧烈震荡，带积分限幅与 sign-flip 重置防止 windup
- **自然背压 (Natural Backpressure)**: 启用背压时，通过 `ArrayBlockingQueue.put()` 让生产者线程在队列满时自动阻塞，配合 Java 21 虚拟线程消费者，消除主动轮询 (`Thread.sleep`) 和伪令牌桶开销
- **多级限流策略**: 根据延迟误差映射为 NORMAL / MEDIUM / HIGH / CRITICAL 等级

## 快速开始

### 基础流处理

```java
StreamBuilder builder = new StreamBuilder("my-job");

builder.fromCollection(data)
    .filter(n -> n % 2 == 0)
    .map(n -> n * n)
    .addSink(new ConsoleSink<>())
    .execute();
```

### 窗口聚合

```java
builder.fromCollection(sensorData)
    .keyBy(SensorReading::getSensorId)
    .window(WindowAssigner.tumblingTimeWindow(Duration.ofSeconds(10)))
    .aggregate(AggregateFunction.average())
    .addSink(new ConsoleSink<>())
    .execute();
```

### 带背压的执行

背压与自适应窗口默认启用（`StreamConfig` 默认 `enableBackpressure = true`，`enableAdaptiveWindow = true`）。
创建 `DataStreamImpl` 后可直接执行：

```java
StreamBuilder builder = new StreamBuilder("backpressure-job");
builder.fromCollection(largeDataSet)
    .map(this::process)
    .addSink(sink)
    .execute();
```

当背压启用时，引擎内部使用 **生产者-消费者 Pipeline**（`BlockingQueue` + 虚拟线程），队列满时主线程自然阻塞，实现零轮询背压。

## 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                      Streaming Data Engine                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │  DataSource │───▶│  Operators  │───▶│   DataSink  │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
├─────────────────────────────────────────────────────────────────┤
│                         核心组件层                               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │  Execution  │    │   Window    │    │  Watermark  │         │
│  │   Engine    │    │   Manager   │    │   Manager   │         │
│  │ (Virtual    │    │  (Adaptive) │    │(Multi-Part) │         │
│  │  Threads)   │    │             │    │             │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
├─────────────────────────────────────────────────────────────────┤
│                         存储层 (LSM-Tree)                        │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   MemTable  │───▶│  Immutable  │───▶│   Segment   │         │
│  │  (Memory)   │    │  MemTable   │    │   (Disk)    │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
├─────────────────────────────────────────────────────────────────┤
│                         AI 智能控制层                            │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Adaptive  │    │   Anomaly   │    │ Backpressure│         │
│  │    Window   │    │  Detector   │    │ Controller  │         │
│  │ (Little's   │    │  (3-sigma)  │    │(EWMA + PID +│         │
│  │   Law+PID)  │    │             │    │ BlockingQueue)│       │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

## 关键设计细节

### 自然背压 Pipeline

`ExecutionEngine` 在启用背压时创建内部 `Pipeline`：

- `ArrayBlockingQueue` 作为有界缓冲区
- `parallelism` 个虚拟线程消费者从队列 `take()` 并执行算子链
- 生产者通过 `queue.put(record)` 提交数据，队列满时自动阻塞
- 使用 **毒丸 (Poison Pill)** 信号优雅关闭消费者线程

### PID 抗 Windup

背压控制器中的 PID 配备两项保护机制：
1. **积分限幅 (Integral Clamp)**: 积分项被限制在 `[-30, 30]`，避免高压期累积过深
2. **符号翻转重置 (Sign-Flip Reset)**: 当延迟误差从负（过载）跨过 0 变为正（欠载）时，自动重置 PID 积分器，确保系统恢复后限流能够快速放宽

## 性能指标

| 指标 | 数值 |
|------|------|
| LSM-Tree 读取吞吐量 | 100K+ ops/秒 |
| 背压控制器并发采样 | 100K+ ops/秒 |
| 端到端平均延迟 | < 20ms |
| 每条记录内存占用 | < 1KB |
| 乱序容忍 | 可配置，默认 5 秒 |

## 设计思想

### 1. 借鉴 Kafka
- **顺序写入**: LSM-Tree 追加写优化
- **分区分段**: 数据分片并行处理
- **日志压缩**: Compaction 合并小文件

### 2. 借鉴 Flink
- **事件时间**: Watermark 推进时间语义
- **窗口计算**: 灵活的窗口分配和触发
- **状态管理**: 有状态算子支持

### 3. 借鉴 ClickHouse
- **列式存储**: 高效的聚合查询
- **MergeTree**: 分区+排序+索引
- **向量化执行**: 批量处理优化

## 棘手问题解决方案

### 海量数据实时聚合的延迟与准确性权衡

**问题**: 大规模流数据实时聚合时，如何平衡计算延迟和结果准确性？

**解决方案**:
1. **增量聚合**: MergeTree 引擎预聚合，减少实时计算量
2. **分层窗口**: 大窗口用于准确结果，小窗口用于低延迟
3. **AI 自适应**: 根据流量特征自动调整窗口大小
4. **迟到数据处理**: Watermark + Allowed Lateness 机制

### PID 积分 Windup 导致背压无法恢复

**问题**: 高压力阶段 PID 积分项持续累积负值，即使延迟已经恢复，积分器仍拖拽 `predicted` 为负数，限流卡在 `MIN_RATE_LIMIT` 无法放宽。

**解决方案**:
1. 在 `PIDController` 中增加 `integralClamp`，将积分限制在合理范围
2. 在 `BackpressureController` 中监测延迟误差符号翻转 (`lastLatencyError <= 0 && latencyError > 0`)，触发 `pidController.reset()` 清零历史累积

## 新增特性（2024 Q4）

### 1. Checkpoint 容错机制
- **Barrier 对齐**: 参考 Flink 的 Chandy-Lamport 算法实现多输入算子 Barrier 对齐
- **状态快照**: 支持算子级状态快照和恢复
- **两阶段提交**: 与 Kafka Sink 集成实现 Exactly-Once

### 2. 分区级 Watermark 管理
- **多分区独立 Watermark**: 每个数据源分区维护独立 Watermark
- **全局 Watermark 计算**: 取活跃分区最小值保证数据完整性
- **空闲分区检测**: 自动检测并处理空闲分区防止 Watermark 停滞

### 3. Kafka 连接器
- **Kafka Source**: 消费者组并行消费、自动重平衡、offset 管理
- **Kafka Sink**: 异步批量写入、事务支持、两阶段提交
- **Exactly-Once 语义**: 端到端精确一次处理

### 4. 持久化状态后端
- **PersistentLSMTree**: 真正持久化到指定目录（非临时文件）
- **WAL 恢复**: 启动时从 WAL 恢复未刷盘数据
- **Segment 加载**: 自动加载已有的磁盘 Segment 文件

## 应用封装示例

```java
// 实时订单分析应用
RealTimeOrderAnalyticsApp app = RealTimeOrderAnalyticsApp.builder()
    .bootstrapServers("localhost:9092")
    .inputTopic("orders")
    .outputTopic("order-stats")
    .build();

app.start();
```

## 项目结构

```
src/main/java/com/kxj/streamingdataengine/
├── app/                    # 应用层（封装示例）
├── checkpoint/             # Checkpoint 容错机制
│   ├── CheckpointBarrier.java
│   ├── AligningBarrierHandler.java    # Barrier 对齐核心
│   └── AbstractMultiInputOperator.java
├── connector/kafka/        # Kafka 连接器
│   ├── KafkaSource.java
│   ├── KafkaSink.java
│   └── KafkaConnectorConfig.java
├── core/                   # 运行时核心
│   ├── model/              # 数据模型
│   ├── operator/           # 算子接口
│   └── watermark/          # Watermark 管理
│       ├── WatermarkStrategy.java
│       └── PartitionedWatermarkManager.java  # 分区 Watermark
├── storage/lsm/            # 存储层
│   ├── LSMTree.java        # 基础 LSM-Tree
│   ├── PersistentLSMTree.java            # 持久化版本
│   └── WriteAheadLog.java
├── ai/                     # 智能控制层
│   ├── AdaptiveWindowManager.java
│   ├── AnomalyDetector.java
│   └── BackpressureController.java
└── stream/                 # DSL 层
    ├── StreamBuilder.java
    └── DataStreamImpl.java
```

## 学习价值

本项目适合作为**流式处理框架学习**的实习项目：

1. **架构设计**: 分层架构（DSL → Core → Storage）
2. **算法实现**: 
   - Chandy-Lamport 分布式快照（简化版）
   - LSM-Tree 存储引擎
   - PID 控制器 + Little's Law
3. **工程实践**:
   - Java 21 虚拟线程
   - 设计模式（Builder、Strategy、Template Method）
   - 测试策略（单元、集成、属性测试）

## 扩展计划

- [ ] SQL 查询接口（Calcite 集成）
- [ ] 更多数据源连接器（Pulsar、RocketMQ）
- [ ] Web UI 监控面板（Grafana 集成）
- [ ] 机器学习集成（实时模型推理）
