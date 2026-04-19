package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;

/**
 * 计数聚合实现
 */
public class CountAggregate<T> implements AggregateFunction<T, CountAccumulator, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    @Override
    public void add(T value, CountAccumulator accumulator) {
        accumulator.count++;
    }

    @Override
    public Long getResult(CountAccumulator accumulator) {
        return accumulator.count;
    }

    @Override
    public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
        a.count += b.count;
        return a;
    }
}
