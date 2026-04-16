package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;

/**
 * 浮点数求和聚合实现
 */
public class SumDoubleAggregate implements AggregateFunction<Double, DoubleSumAccumulator, Double> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public DoubleSumAccumulator createAccumulator() {
        return new DoubleSumAccumulator();
    }

    @Override
    public void add(Double value, DoubleSumAccumulator accumulator) {
        accumulator.sum += (value != null ? value : 0.0);
    }

    @Override
    public Double getResult(DoubleSumAccumulator accumulator) {
        return accumulator.sum;
    }

    @Override
    public DoubleSumAccumulator merge(DoubleSumAccumulator a, DoubleSumAccumulator b) {
        a.sum += b.sum;
        return a;
    }
}
