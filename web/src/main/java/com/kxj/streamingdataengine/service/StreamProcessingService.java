package com.kxj.streamingdataengine.service;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.DataStream;
import com.kxj.streamingdataengine.sink.CollectSink;
import com.kxj.streamingdataengine.stream.StreamBuilder;
import com.kxj.streamingdataengine.window.WindowAssigner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.Serial;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 流处理服务
 * 封装各种流处理场景
 */
@Slf4j
@Service
public class StreamProcessingService {

    // 存储最近的处理结果
    private final Map<String, List<Map<String, Object>>> recentResults = new ConcurrentHashMap<>();

    /**
     * 基础过滤和转换演示
     */
    public Map<String, Object> processBasicDemo() {
        log.info("[kxj: 执行基础流处理演示]");

        List<Integer> data = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        CollectSink<String> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("basic-demo");
        builder.fromCollection(data)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .map(n -> "平方结果: " + n)
                .addSink(sink)
                .execute();

        Map<String, Object> result = Map.of(
                "input", data,
                "output", sink.getCollected(),
                "description", "过滤偶数并计算平方",
                "processedAt", System.currentTimeMillis()
        );

        recentResults.put("basic", List.of(result));
        return result;
    }

    /**
     * 聚合统计演示
     */
    public Map<String, Object> processAggregateDemo(int count) {
        log.info("[kxj: 执行聚合统计演示] dataCount={}", count);

        List<Double> temperatures = generateTemperatureData(count);
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

        Map<String, Object> result = Map.of(
                "dataCount", temperatures.size(),
                "statistics", stats,
                "sampleData", temperatures.subList(0, Math.min(10, temperatures.size())),
                "description", "温度数据聚合统计",
                "processedAt", System.currentTimeMillis()
        );

        recentResults.put("aggregate", List.of(result));
        return result;
    }

    /**
     * IoT传感器数据模拟和窗口聚合
     */
    public Map<String, Object> processIoTDemo(int sensorCount, int readingsPerSensor) {
        log.info("[kxj: 执行IoT传感器数据演示] sensors={}, readings={}", sensorCount, readingsPerSensor);

        // 生成模拟传感器数据
        List<SensorReading> readings = generateSensorData(sensorCount, readingsPerSensor);

        // 按传感器ID分组聚合
        Map<String, List<SensorReading>> groupedBySensor = readings.stream()
                .collect(Collectors.groupingBy(SensorReading::sensorId));

        List<Map<String, Object>> sensorStats = new ArrayList<>();

        for (Map.Entry<String, List<SensorReading>> entry : groupedBySensor.entrySet()) {
            String sensorId = entry.getKey();
            List<Double> values = entry.getValue().stream()
                    .map(SensorReading::value)
                    .toList();

            CollectSink<Map<String, Object>> sink = new CollectSink<>();

            StreamBuilder builder = new StreamBuilder("iot-sensor-" + sensorId);
            builder.fromCollection(values)
                    .keyBy(v -> sensorId)
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

            sensorStats.add(Map.of(
                    "sensorId", sensorId,
                    "readingsCount", values.size(),
                    "statistics", stats,
                    "latestReading", entry.getValue().get(entry.getValue().size() - 1)
            ));
        }

        Map<String, Object> result = Map.of(
                "sensorCount", sensorCount,
                "totalReadings", readings.size(),
                "sensorStats", sensorStats,
                "description", "IoT传感器数据实时聚合",
                "processedAt", System.currentTimeMillis()
        );

        recentResults.put("iot", List.of(result));
        return result;
    }

    /**
     * 自定义数据处理
     */
    public Map<String, Object> processCustomData(List<Map<String, Object>> data) {
        long startTime = System.currentTimeMillis();
        log.info("[kxj: 执行自定义数据处理] inputCount={}", data.size());

        CollectSink<Map<String, Object>> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("custom-process-job");
        builder.fromCollection(data)
                .filter(m -> m.get("value") != null)
                .map(m -> {
                    Map<String, Object> result = new HashMap<>(m);
                    result.put("processed", true);
                    result.put("timestamp", System.currentTimeMillis());

                    // 如果 value 是数字，计算平方
                    Object value = m.get("value");
                    if (value instanceof Number num) {
                        double squared = num.doubleValue() * num.doubleValue();
                        result.put("squared", squared);
                    }

                    return result;
                })
                .addSink(sink)
                .execute();

        long duration = System.currentTimeMillis() - startTime;

        Map<String, Object> result = Map.of(
                "inputCount", data.size(),
                "outputCount", sink.getCollected().size(),
                "durationMs", duration,
                "throughput", String.format("%.2f", data.size() * 1000.0 / duration) + " records/s",
                "processedAt", System.currentTimeMillis(),
                "sampleOutput", sink.getCollected().subList(0, Math.min(5, sink.getCollected().size()))
        );

        recentResults.put("custom", List.of(result));
        return result;
    }

    /**
     * 获取引擎状态
     */
    public Map<String, Object> getEngineStatus() {
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
                        "freeMB", freeMemory,
                        "usagePercent", String.format("%.1f%%", (usedMemory * 100.0 / maxMemory))
                ),
                "processors", runtime.availableProcessors(),
                "timestamp", System.currentTimeMillis()
        );
    }

    /**
     * 获取最近的处理结果
     */
    public Map<String, Object> getRecentResults() {
        return Map.of(
                "recentResults", recentResults,
                "resultCount", recentResults.size(),
                "timestamp", System.currentTimeMillis()
        );
    }

    // ===== 辅助方法 =====

    private List<Double> generateTemperatureData(int count) {
        Random random = new Random();
        List<Double> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            data.add(20 + random.nextDouble() * 15);
        }
        return data;
    }

    private List<SensorReading> generateSensorData(int sensorCount, int readingsPerSensor) {
        Random random = new Random();
        List<SensorReading> readings = new ArrayList<>();

        for (int s = 0; s < sensorCount; s++) {
            String sensorId = "SENSOR-" + String.format("%03d", s + 1);
            double baseTemp = 20 + random.nextDouble() * 10;

            for (int r = 0; r < readingsPerSensor; r++) {
                double value = baseTemp + random.nextGaussian() * 2;
                long timestamp = System.currentTimeMillis() - (readingsPerSensor - r) * 1000L;
                readings.add(new SensorReading(sensorId, value, timestamp));
            }
        }

        return readings;
    }

    // ===== 内部类 =====

    private record SensorReading(String sensorId, double value, long timestamp) {
    }

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
                    "min", String.format("%.2f", min == Double.MAX_VALUE ? 0.0 : min),
                    "count", count
            );
        }
    }
}
