# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

高性能流式数据处理引擎 (High-performance Streaming Data Engine). A Java-based lightweight stream processing engine inspired by Kafka (log-structured storage), Flink (event time / windows / watermarks), and ClickHouse (MergeTree-style incremental aggregation).

- **Language**: Java 21
- **Build Tool**: Maven (wrapper: `./mvnw`)
- **Framework**: Spring Boot 3.5.13 (web starter)
- **Utilities**: Lombok (`@Slf4j`, `@RequiredArgsConstructor`, `@Getter`)

## Common Commands

```bash
# Compile
./mvnw compile

# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=IoTScenarioTest

# Run a single test method
./mvnw test -Dtest=IoTScenarioTest#testSensorDataAggregation

# Run the Spring Boot application
./mvnw spring-boot:run
```

## Architecture

### Entry Flow
`StreamBuilder` creates a `DataStreamImpl`, which builds a chain of `Function<?, ?>` transformations. Calling `execute()` instantiates `ExecutionEngine`, converts the function chain into `StreamOperator`s, reads records from `DataSource`, and pushes results through the engine to `DataSink`s.

### Execution Engine
`ExecutionEngine` is the central scheduler:
- Uses `Executors.newVirtualThreadPerTaskExecutor()` for all async work.
- Coordinates `WatermarkManager` (per-partition watermark tracking, global watermark = min of active partitions), `AdaptiveWindowManager` (dynamic window sizing), `AnomalyDetector` (3-sigma anomaly detection), and `BackpressureController` (load-aware rate limiting).
- `processRecord` applies the operator chain, handles watermarks, samples for adaptive/AI features, and writes to sinks.

### Storage Layer (LSM-Tree)
`LSMTree<K, V>` in `storage.lsm` implements a log-structured merge-tree:
- `MemTable` (active + immutable) backed by `ConcurrentSkipListMap`.
- `Segment` for on-disk immutable storage with sparse indexing.
- `WriteAheadLog` for durability.
- `CompactionStrategy` (size-tiered / leveled) merges segments in the background.

### Windowing
`WindowAssigner` in `window.WindowAssigner` provides tumbling, sliding, session, global, and count-based windows. `WindowedStreamImpl` and `KeyedStreamImpl` are inner classes of `DataStreamImpl`. Default triggers: `EventTimeTrigger` for time windows, `ProcessingTimeTrigger` for global/count windows.

### AI Control Layer
- `AdaptiveWindowManager`: Uses **Little's Law (L = λW) + PID controller** to predict and smooth window size adjustments based on arrival rate and latency percentiles. Also dynamically adjusts `maxOutOfOrderness`.
- `AnomalyDetector`: 3-sigma statistical detection with change-rate and periodicity checks.
- `BackpressureController`: Severity-level based rate limiting (NORMAL / MEDIUM / HIGH / CRITICAL thresholds) plus PID fine-tuning that only relaxes the limit when latency is below target.

### REST API
`StreamingController` exposes demo endpoints under `/api/stream`:
- `GET /api/stream/demo/basic` — basic filter+map demo.
- `GET /api/stream/demo/aggregate/{num}` — global aggregation demo.
- `POST /api/stream/process` — process a list of maps through the engine.
- `GET /api/stream/status` — engine status and memory info.

## Code Conventions

- **Logging**: Key logic and data transformations are logged with the `[kxj: ...]` prefix pattern, e.g. `log.info("[kxj: 执行引擎启动] parallelism=...");`.
- **Virtual Threads**: Prefer `Thread.ofVirtual()` and `Executors.newVirtualThreadPerTaskExecutor()` for concurrency.
- **Immutable Streams**: `map` / `filter` return new `DataStreamImpl` instances with copied transformation chains.
- **Records**: Use Java `record` for simple data carriers (e.g., `EngineStatus`, `StreamRecord`).
- **Dependency Injection**: Use `@RequiredArgsConstructor` + `final` fields; avoid `@Autowired`.

## Test Organization

- **Scenario tests**: `scenario.IoTScenarioTest`, `ECommerceScenarioTest`, `FinancialScenarioTest` — end-to-end usage demos.
- **AI tests**: `ai.AdaptiveWindowManagerTest`, `AnomalyDetectorTest`, `BackpressureControllerTest`.
- **Storage tests**: `storage.lsm.LSMTreeTest`.
- **Performance**: `performance.PerformanceBenchmarkTest`.

## Important File Paths

- `src/main/java/com/kxj/streamingdataengine/stream/StreamBuilder.java` — API entry point.
- `src/main/java/com/kxj/streamingdataengine/stream/DataStreamImpl.java` — stream DSL implementation.
- `src/main/java/com/kxj/streamingdataengine/stream/StreamConfig.java` — engine configuration (parallelism, watermark interval, adaptive window, backpressure).
- `src/main/java/com/kxj/streamingdataengine/execution/ExecutionEngine.java` — core scheduler.
- `src/main/java/com/kxj/streamingdataengine/execution/WatermarkManager.java` — multi-partition watermark tracking.
- `src/main/java/com/kxj/streamingdataengine/storage/lsm/LSMTree.java` — storage engine.
- `src/main/java/com/kxj/streamingdataengine/window/WindowAssigner.java` — window factory methods.
- `src/main/java/com/kxj/streamingdataengine/controller/StreamingController.java` — REST demo endpoints.
- `src/main/java/com/kxj/streamingdataengine/sink/CollectSink.java` — test-friendly sink that collects outputs into a list.
- `src/main/resources/logback-spring.xml` — logging config.
- `src/main/resources/application.yaml` — minimal Spring Boot config.
