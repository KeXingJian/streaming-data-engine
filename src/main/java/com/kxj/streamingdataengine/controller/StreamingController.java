package com.kxj.streamingdataengine.controller;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.sink.CollectSink;
import com.kxj.streamingdataengine.sink.ConsoleSink;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
    @GetMapping("/demo/aggregate")
    public Map<String, Object> aggregateDemo() {
        List<Double> temperatures = generateTemperatureData(100);

        // 手动计算统计
        double sum = temperatures.stream().mapToDouble(Double::doubleValue).sum();
        double avg = sum / temperatures.size();
        double max = temperatures.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        double min = temperatures.stream().mapToDouble(Double::doubleValue).min().orElse(0);

        return Map.of(
                "dataCount", temperatures.size(),
                "statistics", Map.of(
                        "sum", String.format("%.2f", sum),
                        "average", String.format("%.2f", avg),
                        "max", String.format("%.2f", max),
                        "min", String.format("%.2f", min)
                ),
                "sampleData", temperatures.subList(0, 10)
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
}
