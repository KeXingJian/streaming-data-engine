package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;

/**
 * 最小值聚合实现
 */
public class MinAggregate<T extends Comparable<T>> implements AggregateFunction<T, Holder<T>, T> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Holder<T> createAccumulator() {
        return new Holder<>(null);
    }

    @Override
    public void add(T value, Holder<T> accumulator) {
        if (accumulator.value == null || value.compareTo(accumulator.value) < 0) {
            accumulator.value = value;
        }
    }

    @Override
    public T getResult(Holder<T> accumulator) {
        return accumulator.value;
    }

    @Override
    public Holder<T> merge(Holder<T> a, Holder<T> b) {
        if (a.value == null) return b;
        if (b.value == null) return a;
        return a.value.compareTo(b.value) < 0 ? a : b;
    }
}
