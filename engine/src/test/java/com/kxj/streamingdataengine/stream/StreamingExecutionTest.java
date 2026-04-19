package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.watermark.WatermarkStrategies;
import com.kxj.streamingdataengine.sink.CollectSink;
import com.kxj.streamingdataengine.window.WindowAssigner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 端到端流式执行测试
 */
class StreamingExecutionTest {

    private static StreamConfig testConfig() {
        StreamConfig config = new StreamConfig();
        config.setEnableBackpressure(false);
        config.setWatermarkInterval(50);
        config.setEnableAdaptiveWindow(false);
        return config;
    }

    @Test
    @DisplayName("fromCollection -> map -> keyBy -> aggregate -> sink")
    void testKeyedAggregateExecution() {
        List<Integer> data = new ArrayList<>(List.of(1, 2, 3, 4, 1, 2));

        CollectSink<Long> sink = new CollectSink<>();

        DataStreamImpl<Integer> stream = new DataStreamImpl<>(
                "keyed-test",
                new CollectionSource<>(data, WatermarkStrategies.forMonotonousTimestamps(v -> 0L), Duration.ofMillis(50)),
                testConfig());
        stream.map(v -> v)
                .keyBy(v -> v % 2 == 0 ? "even" : "odd")
                .aggregate(new SumAggregateFunction())
                .addSink(sink)
                .execute();

        List<Long> results = sink.getCollected().stream().sorted().toList();
        // 逐条处理，每条输入都会输出对应 key 的当前聚合结果
        // 1(odd=1), 2(even=2), 3(odd=4), 4(even=6), 1(odd=5), 2(even=8)
        assertEquals(List.of(1L, 2L, 4L, 5L, 6L, 8L), results);
    }

    @Test
    @DisplayName("fromCollection -> window -> aggregate -> sink，验证 watermark 驱动触发")
    void testWindowAggregateExecution() {
        long baseTime = 0L;
        // 窗口大小 1000ms，事件时间 500 和 800 属于窗口 [0,1000)
        List<Integer> data = new ArrayList<>();
        data.add(10); // eventTime = 500
        data.add(20); // eventTime = 800
        data.add(30); // eventTime = 1500

        CollectSink<Long> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("window-test");
        DataStreamImpl<Integer> stream = builder.fromCollection(data,
                        WatermarkStrategies.forMonotonousTimestamps(v -> {
                            int idx = data.indexOf(v);
                            return baseTime + (idx == 0 ? 500L : idx == 1 ? 800L : 1500L);
                        }), Duration.ofMillis(50))
                .withConfig(testConfig());

        stream.window(WindowAssigner.tumblingTimeWindow(Duration.ofMillis(1000)))
                .aggregate(new SumAggregateFunction())
                .addSink(sink)
                .execute();

        List<Long> results = sink.getCollected().stream().sorted().toList();
        // 窗口 [0,1000) 输出 10+20=30， watermark=1500 触发 [0,1000)，但 [1000,2000) 尚未触发
        assertEquals(List.of(30L), results);
    }

    @Test
    @DisplayName("window -> reduce 端到端测试")
    void testWindowReduceExecution() {
        long baseTime = 0L;
        List<Integer> data = new ArrayList<>();
        data.add(5);  // eventTime = 100
        data.add(3);  // eventTime = 200
        data.add(8);  // eventTime = 1200

        CollectSink<Integer> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("window-reduce-test");
        DataStreamImpl<Integer> stream = builder.fromCollection(data,
                        WatermarkStrategies.forMonotonousTimestamps(v -> {
                            int idx = data.indexOf(v);
                            return baseTime + (idx == 0 ? 100L : idx == 1 ? 200L : 1200L);
                        }), Duration.ofMillis(50))
                .withConfig(testConfig());

        stream.window(WindowAssigner.tumblingTimeWindow(Duration.ofMillis(1000)))
                .reduce(Math::max)
                .addSink(sink)
                .execute();

        List<Integer> results = sink.getCollected().stream().sorted().toList();
        assertEquals(List.of(5), results); // max(5,3)=5
    }

    @Test
    @DisplayName("keyBy -> window -> aggregate keyed-window 端到端测试")
    void testKeyedWindowAggregateExecution() {
        long baseTime = 0L;
        List<Integer> data = new ArrayList<>();
        data.add(10); // eventTime = 100 -> window [0, 500)
        data.add(20); // eventTime = 200 -> window [0, 500)
        data.add(30); // eventTime = 500 -> window [500, 1000)

        CollectSink<Long> sink = new CollectSink<>();

        StreamBuilder builder = new StreamBuilder("keyed-window-test");
        DataStreamImpl<Integer> stream = builder.fromCollection(data,
                        WatermarkStrategies.forMonotonousTimestamps(v -> {
                            int idx = data.indexOf(v);
                            return baseTime + (idx == 0 ? 100L : idx == 1 ? 200L : 500L);
                        }), Duration.ofMillis(50))
                .withConfig(testConfig());

        // 使用 500ms 窗口，使得 watermark=500 可以触发窗口 [0, 500)
        stream.keyBy(v -> v > 15 ? "A" : "B")
                .window(WindowAssigner.tumblingTimeWindow(Duration.ofMillis(500)))
                .aggregate(new SumAggregateFunction())
                .addSink(sink)
                .execute();

        List<Long> results = sink.getCollected().stream().sorted().toList();
        // 窗口 [0, 500): B: 10, A: 20
        assertEquals(List.of(10L, 20L), results);
    }

    // ========== 辅助类和聚合函数 ==========

    private static class SumAccumulator {
        long sum;
        void add(int v) { sum += v; }
    }

    private static class SumAggregateFunction implements AggregateFunction<Integer, SumAccumulator, Long> {
        @Override
        public SumAccumulator createAccumulator() {
            return new SumAccumulator();
        }

        @Override
        public void add(Integer value, SumAccumulator accumulator) {
            accumulator.add(value);
        }

        @Override
        public Long getResult(SumAccumulator accumulator) {
            return accumulator.sum;
        }

        @Override
        public SumAccumulator merge(SumAccumulator a, SumAccumulator b) {
            a.sum += b.sum;
            return a;
        }
    }
}
