package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 聚合函数接口
 * 借鉴Flink的AggregateFunction设计
 *
 * @param <T> 输入类型
 * @param <ACC> 累加器类型
 * @param <R> 结果类型
 */
public interface AggregateFunction<T, ACC, R> extends Serializable {

    /**
     * 创建新的累加器
     */
    ACC createAccumulator();

    /**
     * 添加值到累加器
     */
    void add(T value, ACC accumulator);

    /**
     * 获取累加器结果
     */
    R getResult(ACC accumulator);

    /**
     * 合并两个累加器
     */
    ACC merge(ACC a, ACC b);

    /**
     * 增量聚合：部分聚合结果合并
     * 用于分布式场景
     */
    default ACC combine(Iterable<ACC> accumulators) {
        ACC result = null;
        for (ACC acc : accumulators) {
            if (result == null) {
                result = acc;
            } else {
                result = merge(result, acc);
            }
        }
        return result;
    }

    // ============== 内置聚合函数 ==============

    /**
     * 计数聚合
     */
    static <T> AggregateFunction<T, CountAccumulator, Long> count() {
        return new CountAggregate<>();
    }

    /**
     * 求和聚合（整数）
     */
    static AggregateFunction<Integer, SumAccumulator, Long> sumInt() {
        return new SumIntAggregate();
    }

    /**
     * 求和聚合（长整数）
     */
    static AggregateFunction<Long, SumAccumulator, Long> sumLong() {
        return new SumLongAggregate();
    }

    /**
     * 求和聚合（浮点数）
     */
    static AggregateFunction<Double, DoubleSumAccumulator, Double> sumDouble() {
        return new SumDoubleAggregate();
    }

    /**
     * 平均值聚合
     */
    static AggregateFunction<Double, AverageAccumulator, Double> average() {
        return new AverageAggregate();
    }

    /**
     * 最大值聚合
     */
    static <T extends Comparable<T>> AggregateFunction<T, Holder<T>, T> max() {
        return new MaxAggregate<>();
    }

    /**
     * 最小值聚合
     */
    static <T extends Comparable<T>> AggregateFunction<T, Holder<T>, T> min() {
        return new MinAggregate<>();
    }
}

// 可变计数累加器
class CountAccumulator implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long count;

    CountAccumulator() {
        this.count = 0;
    }
}

// 计数聚合实现
class CountAggregate<T> implements AggregateFunction<T, CountAccumulator, Long> {
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

// 可变求和累加器
class SumAccumulator implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long sum;

    SumAccumulator() {
        this.sum = 0;
    }
}

// 整数求和
class SumIntAggregate implements AggregateFunction<Integer, SumAccumulator, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public SumAccumulator createAccumulator() {
        return new SumAccumulator();
    }

    @Override
    public void add(Integer value, SumAccumulator accumulator) {
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

// 长整数求和
class SumLongAggregate implements AggregateFunction<Long, SumAccumulator, Long> {
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

// 可变浮点数求和累加器
class DoubleSumAccumulator implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    double sum;

    DoubleSumAccumulator() {
        this.sum = 0.0;
    }
}

// 浮点数求和
class SumDoubleAggregate implements AggregateFunction<Double, DoubleSumAccumulator, Double> {
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

// 平均值累加器
class AverageAccumulator implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long count;
    double sum;

    AverageAccumulator() {
        this.count = 0;
        this.sum = 0.0;
    }

    void add(double value) {
        count++;
        sum += value;
    }

    double getAverage() {
        return count == 0 ? 0.0 : sum / count;
    }

    void merge(AverageAccumulator other) {
        this.count += other.count;
        this.sum += other.sum;
    }
}

// 平均值聚合
class AverageAggregate implements AggregateFunction<Double, AverageAccumulator, Double> {
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

// 可变包装器
class Holder<T> implements java.io.Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    T value;

    Holder(T value) {
        this.value = value;
    }
}

// 最大值聚合
class MaxAggregate<T extends Comparable<T>> implements AggregateFunction<T, Holder<T>, T> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Holder<T> createAccumulator() {
        return new Holder<>(null);
    }

    @Override
    public void add(T value, Holder<T> accumulator) {
        if (accumulator.value == null || value.compareTo(accumulator.value) > 0) {
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
        return a.value.compareTo(b.value) > 0 ? a : b;
    }
}

// 最小值聚合
class MinAggregate<T extends Comparable<T>> implements AggregateFunction<T, Holder<T>, T> {
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
