package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KeyedAggregateOperator 单元测试
 */
class KeyedAggregateOperatorTest {

    @Test
    @DisplayName("逐条输入后即时输出对应 key 的聚合结果")
    @SuppressWarnings("unchecked")
    void testProcessElementReturnsCurrentAggregate() {
        KeyedAggregateOperator<Integer, String, SumAccumulator, Long> operator =
                new KeyedAggregateOperator<>(
                        i -> i % 2 == 0 ? "even" : "odd",
                        new SumAggregateFunction()
                );

        StreamRecord<Integer> r1 = new StreamRecord<>("k1", 1, 0, 0, 0);
        StreamRecord<Integer> r2 = new StreamRecord<>("k2", 2, 0, 0, 0);
        StreamRecord<Integer> r3 = new StreamRecord<>("k3", 3, 0, 0, 0);
        StreamRecord<Integer> r4 = new StreamRecord<>("k4", 4, 0, 0, 0);

        List<StreamRecord<?>> out1 = (List<StreamRecord<?>>) (List<?>) operator.processElement(r1);
        assertEquals(1, out1.size());
        assertEquals(1L, ((Number) out1.get(0).value()).longValue());

        List<StreamRecord<?>> out2 = (List<StreamRecord<?>>) (List<?>) operator.processElement(r2);
        assertEquals(1, out2.size());
        assertEquals(2L, ((Number) out2.get(0).value()).longValue());

        List<StreamRecord<?>> out3 = (List<StreamRecord<?>>) (List<?>) operator.processElement(r3);
        assertEquals(1, out3.size());
        assertEquals(4L, ((Number) out3.get(0).value()).longValue()); // odd: 1+3

        List<StreamRecord<?>> out4 = (List<StreamRecord<?>>) (List<?>) operator.processElement(r4);
        assertEquals(1, out4.size());
        assertEquals(6L, ((Number) out4.get(0).value()).longValue()); // even: 2+4
    }

    @Test
    @DisplayName("多线程并发输入下状态一致性")
    @SuppressWarnings("unchecked")
    void testConcurrentAccess() throws InterruptedException {
        KeyedAggregateOperator<Integer, String, SumAccumulator, Long> operator =
                new KeyedAggregateOperator<>(
                        i -> "sameKey",
                        new SumAggregateFunction()
                );

        int threads = 10;
        int perThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger emitted = new AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        StreamRecord<Integer> r = new StreamRecord<>(null, 1, 0, 0, 0);
                        List<StreamRecord<?>> out = (List<StreamRecord<?>>) (List<?>) operator.processElement(r);
                        emitted.addAndGet(out.size());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threads * perThread, emitted.get());

        // 验证最终状态
        StreamRecord<Integer> finalRecord = new StreamRecord<>(null, 0, 0, 0, 0);
        List<StreamRecord<?>> out = (List<StreamRecord<?>>) (List<?>) operator.processElement(finalRecord);
        assertEquals(1, out.size());
        assertEquals((long) threads * perThread, ((Number) out.get(0).value()).longValue());
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
