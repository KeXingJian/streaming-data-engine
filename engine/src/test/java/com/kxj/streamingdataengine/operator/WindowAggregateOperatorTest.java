package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.WindowAssigner;
import com.kxj.streamingdataengine.window.trigger.EventTimeTrigger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WindowAggregateOperator 单元测试
 */
class WindowAggregateOperatorTest {

    private static final long WINDOW_SIZE = 1000L;

    private Window.Assigner<Integer> assigner = WindowAssigner.tumblingTimeWindow(Duration.ofMillis(WINDOW_SIZE));

    @Test
    @DisplayName("Tumbling Window 按 eventTime 分配")
    void testTumblingWindowAssignment() {
        WindowAggregateOperator<Integer, SumAccumulator, Long> operator =
                new WindowAggregateOperator<>(
                        assigner,
                        EventTimeTrigger.create(),
                        new SumAggregateFunction(),
                        null,
                        0
                );

        // 事件时间 500 属于窗口 [0, 1000)
        StreamRecord<Integer> r1 = new StreamRecord<>(null, 10, 500L, 0, 0);
        List<StreamRecord<Integer>> out1 = operator.processElement(r1);
        // EventTimeTrigger.onElement 对于未迟到的元素返回 CONTINUE
        assertTrue(out1.isEmpty());

        // 事件时间 1500 属于窗口 [1000, 2000)
        StreamRecord<Integer> r2 = new StreamRecord<>(null, 20, 1500L, 0, 0);
        List<StreamRecord<Integer>> out2 = operator.processElement(r2);
        assertTrue(out2.isEmpty());
    }

    @Test
    @DisplayName("Watermark 超过窗口结束时间后正确触发并输出结果")
    @SuppressWarnings("unchecked")
    void testWatermarkFiresWindow() {
        WindowAggregateOperator<Integer, SumAccumulator, Long> operator =
                new WindowAggregateOperator<>(
                        assigner,
                        EventTimeTrigger.create(),
                        new SumAggregateFunction(),
                        null,
                        0
                );

        StreamRecord<Integer> r1 = new StreamRecord<>(null, 10, 500L, 0, 0);
        StreamRecord<Integer> r2 = new StreamRecord<>(null, 20, 800L, 0, 0);
        StreamRecord<Integer> r3 = new StreamRecord<>(null, 30, 1500L, 0, 0);

        operator.processElement(r1);
        operator.processElement(r2);
        operator.processElement(r3);

        // Watermark 到达 999，窗口 [0,1000) 的 maxTimestamp=999，EventTimeTrigger.onEventTime(999, [0,1000)) 返回 FIRE_AND_PURGE
        List<StreamRecord<Integer>> fired1 = operator.processWatermark(new Watermark(999L));
        assertEquals(1, fired1.size());
        assertEquals(30L, ((Number) fired1.get(0).value()).longValue()); // 10 + 20

        // Watermark 到达 1000，窗口 [0,1000) 已在 999 时被 purge，不应再输出
        List<StreamRecord<Integer>> fired2 = operator.processWatermark(new Watermark(1000L));
        assertTrue(fired2.isEmpty());

        // 再次发送相同 Watermark，窗口已被清理，不应再输出
        List<StreamRecord<Integer>> fired3 = operator.processWatermark(new Watermark(1000L));
        assertTrue(fired3.isEmpty());
    }

    @Test
    @DisplayName("allowedLateness 对迟到数据的处理")
    @SuppressWarnings("unchecked")
    void testAllowedLateness() {
        WindowAggregateOperator<Integer, SumAccumulator, Long> operator =
                new WindowAggregateOperator<>(
                        assigner,
                        EventTimeTrigger.create(),
                        new SumAggregateFunction(),
                        null,
                        200L // allowedLateness = 200ms
                );

        StreamRecord<Integer> r1 = new StreamRecord<>(null, 10, 500L, 0, 0);
        operator.processElement(r1);

        // Watermark 推进到 999，触发窗口 [0,1000)，结果 10
        List<StreamRecord<?>> fired1 = (List<StreamRecord<?>>) (List<?>) operator.processWatermark(new Watermark(999L));
        assertEquals(1, fired1.size());
        assertEquals(10L, ((Number) fired1.get(0).value()).longValue());

        // 迟到数据：事件时间 800，在 allowedLateness 内（maxTimestamp=999, watermark=999, lateness=200, 999+200=1199 > 800）
        StreamRecord<Integer> lateButAllowed = new StreamRecord<>(null, 5, 800L, 0, 0);
        List<StreamRecord<Integer>> out = operator.processElement(lateButAllowed);
        // 窗口已经被 purge，所以 processElement 只是重新创建窗口并更新，不会立即触发（因为 trigger.onElement 可能返回 CONTINUE）
        // 由于 EventTimeTrigger.onElement 对于未超时的窗口返回 CONTINUE，所以不会立即发射
        assertTrue(out.isEmpty());

        // Watermark 推进到 1200，此时 maxTimestamp(999)+allowedLateness(200)=1199 < 1200，迟到窗口也被清理
        List<StreamRecord<?>> fired2 = (List<StreamRecord<?>>) (List<?>) operator.processWatermark(new Watermark(1200L));
        // 在 999-1200 之间没有新窗口数据，所以不应该有输出
        // 但之前重新创建的 [0,1000) 窗口如果还在的话，这里会被触发。然而我们在 999 时已经 purge 了。
        // processElement(lateButAllowed) 重新创建了 [0,1000) 窗口，所以这里 watermark=1200 会再次触发并 purge
        assertEquals(1, fired2.size());
        assertEquals(5L, ((Number) fired2.get(0).value()).longValue());
    }

    @Test
    @DisplayName("Keyed Window 聚合")
    @SuppressWarnings("unchecked")
    void testKeyedWindowAggregate() {
        WindowAggregateOperator<Integer, SumAccumulator, Long> operator =
                new WindowAggregateOperator<>(
                        assigner,
                        EventTimeTrigger.create(),
                        new SumAggregateFunction(),
                        i -> i > 15 ? "A" : "B",
                        0
                );

        StreamRecord<Integer> r1 = new StreamRecord<>(null, 10, 500L, 0, 0); // B
        StreamRecord<Integer> r2 = new StreamRecord<>(null, 20, 600L, 0, 0); // A
        StreamRecord<Integer> r3 = new StreamRecord<>(null, 30, 700L, 0, 0); // A

        operator.processElement(r1);
        operator.processElement(r2);
        operator.processElement(r3);

        List<StreamRecord<Integer>> fired = operator.processWatermark(new Watermark(999L));
        assertEquals(2, fired.size());
        List<Long> values = fired.stream().map(r -> ((Number) r.value()).longValue()).sorted().toList();
        assertEquals(List.of(10L, 50L), values);
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
