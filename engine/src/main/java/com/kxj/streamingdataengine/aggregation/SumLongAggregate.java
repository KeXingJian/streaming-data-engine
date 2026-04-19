package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;

/**
 * 长整数求和聚合实现
 */
public class SumLongAggregate implements AggregateFunction<Long, SumAccumulator, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public SumAccumulator createAccumulator() {
        return new SumAccumulator();
    }

    @Override
    public void add(Long value, SumAccumulator accumulator) {
        accumulator.sum += (value != null ? value : 0);
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
