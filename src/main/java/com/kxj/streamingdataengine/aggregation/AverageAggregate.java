package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;

/**
 * 平均值聚合实现
 */
public class AverageAggregate implements AggregateFunction<Double, AverageAccumulator, Double> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    @Override
    public void add(Double value, AverageAccumulator accumulator) {
        accumulator.add(value != null ? value : 0.0);
    }

    @Override
    public Double getResult(AverageAccumulator accumulator) {
        return accumulator.getAverage();
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.merge(b);
        return a;
    }
}
