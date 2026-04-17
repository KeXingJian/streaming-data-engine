package com.kxj.streamingdataengine.controller;


import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.sink.CollectSink;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.io.Serial;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流处理引擎REST接口
 */
@RestController
@RequestMapping("/api/stream")
@RequiredArgsConstructor
@Slf4j
public class StreamingController {

    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    /**
     * 基础流处理演示
     */
    @GetMapping("/demo/basic")
    public Map<String, Object> basicDemo() {
        List<Integer> data = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        CollectSink<String> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("basic-demo");
        builder.fromCollection(data)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .map(n -> "平方结果: " + n)
                .addSink(sink)
                .execute();

        return Map.of(
                "input", data,
                "output", sink.getCollected(),
                "description", "过滤偶数并计算平方"
        );
    }

    /**
     * 聚合统计演示
     */
    @GetMapping("/demo/aggregate/{num}")
    public Map<String, Object> aggregateDemo(@PathVariable int num) {
        List<Double> temperatures = generateTemperatureData(num);

        // 使用项目流式引擎做全局聚合
        CollectSink<Map<String, Object>> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("aggregate-demo");
        builder.fromCollection(temperatures)
                .keyBy(t -> "global")
                .aggregate(new AggregateFunction<Double, StatsAccumulator, Map<String, Object>>() {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    public StatsAccumulator createAccumulator() {
                        return new StatsAccumulator();
                    }

                    @Override
                    public void add(Double value, StatsAccumulator accumulator) {
                        accumulator.add(value);
                    }

                    @Override
                    public Map<String, Object> getResult(StatsAccumulator accumulator) {
                        return accumulator.toMap();
                    }

                    @Override
                    public StatsAccumulator merge(StatsAccumulator a, StatsAccumulator b) {
                        a.merge(b);
                        return a;
                    }
                })
                .addSink(sink)
                .execute();

        Map<String, Object> stats = sink.getCollected().isEmpty()
                ? Map.of()
                : sink.getCollected().getFirst();

        return Map.of(
                "dataCount", temperatures.size(),
                "statistics", stats,
                "sampleData", temperatures.subList(0, 10),
                "description", "使用StreamBuilder keyBy+aggregate流式聚合"
        );
    }

    /**
     * 实时数据处理
     */
    @PostMapping("/process")
    public Map<String, Object> processData(@RequestBody List<Map<String, Object>> data) {
        long startTime = System.currentTimeMillis();

        CollectSink<Map<String, Object>> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("process-job");
        builder.fromCollection(data)
                .filter(m -> m.get("value") != null)
                .map(m -> {
                    Map<String, Object> result = new HashMap<>(m);
                    result.put("processed", true);
                    result.put("timestamp", System.currentTimeMillis());
                    return result;
                })
                .addSink(sink)
                .execute();

        long duration = System.currentTimeMillis() - startTime;

        return Map.of(
                "inputCount", data.size(),
                "outputCount", sink.getCollected().size(),
                "durationMs", duration,
                "throughput", String.format("%.2f", data.size() * 1000.0 / duration) + " records/s"
        );
    }

    /**
     * 引擎状态
     */
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory() / 1024 / 1024;
        long totalMemory = runtime.totalMemory() / 1024 / 1024;
        long freeMemory = runtime.freeMemory() / 1024 / 1024;
        long usedMemory = totalMemory - freeMemory;

        return Map.of(
                "engine", "高性能流式数据处理引擎",
                "version", "1.0.0",
                "features", List.of(
                        "LSM-Tree存储",
                        "Watermark乱序处理",
                        "MergeTree增量聚合",
                        "AI自适应窗口",
                        "异常流量检测",
                        "动态背压控制"
                ),
                "memory", Map.of(
                        "maxMB", maxMemory,
                        "usedMB", usedMemory,
                        "freeMB", freeMemory
                ),
                "processors", runtime.availableProcessors()
        );
    }

    /**
     * 生成温度数据
     */
    private List<Double> generateTemperatureData(int count) {
        Random random = new Random();
        List<Double> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            data.add(20 + random.nextDouble() * 15);
        }
        return data;
    }

    /**
     * 统计累加器
     */
    private static class StatsAccumulator implements java.io.Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        double sum;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        long count;

        void add(double value) {
            sum += value;
            max = Math.max(max, value);
            min = Math.min(min, value);
            count++;
        }

        void merge(StatsAccumulator other) {
            sum += other.sum;
            max = Math.max(max, other.max);
            min = Math.min(min, other.min);
            count += other.count;
        }

        Map<String, Object> toMap() {
            return Map.of(
                    "sum", String.format("%.2f", sum),
                    "average", String.format("%.2f", count == 0 ? 0.0 : sum / count),
                    "max", String.format("%.2f", max == Double.MIN_VALUE ? 0.0 : max),
                    "min", String.format("%.2f", min == Double.MAX_VALUE ? 0.0 : min)
            );
        }
    }
}
