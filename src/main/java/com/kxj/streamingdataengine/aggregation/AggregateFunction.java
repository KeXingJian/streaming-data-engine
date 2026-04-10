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
    static <T> AggregateFunction<T, Long, Long> count() {
        return new CountAggregate<>();
    }

    /**
     * 求和聚合（整数）
     */
    static AggregateFunction<Integer, Long, Long> sumInt() {
        return new SumIntAggregate();
    }

    /**
     * 求和聚合（长整数）
     */
    static AggregateFunction<Long, Long, Long> sumLong() {
        return new SumLongAggregate();
    }

    /**
     * 求和聚合（浮点数）
     */
    static AggregateFunction<Double, Double, Double> sumDouble() {
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
    static <T extends Comparable<T>> AggregateFunction<T, T, T> max() {
        return new MaxAggregate<>();
    }

    /**
     * 最小值聚合
     */
    static <T extends Comparable<T>> AggregateFunction<T, T, T> min() {
        return new MinAggregate<>();
    }
}

// 计数聚合实现
class CountAggregate<T> implements AggregateFunction<T, Long, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public void add(T value, Long accumulator) {
        // 不直接使用accumulator（不可变），但保持接口兼容
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

// 整数求和
class SumIntAggregate implements AggregateFunction<Integer, Long, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public void add(Integer value, Long accumulator) {
        // 累加器会被替换
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

// 长整数求和
class SumLongAggregate implements AggregateFunction<Long, Long, Long> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public void add(Long value, Long accumulator) {
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

// 浮点数求和
class SumDoubleAggregate implements AggregateFunction<Double, Double, Double> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public void add(Double value, Double accumulator) {
    }

    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
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

// 最大值聚合
class MaxAggregate<T extends Comparable<T>> implements AggregateFunction<T, T, T> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public T createAccumulator() {
        return null;
    }

    @Override
    public void add(T value, T accumulator) {
        // 不修改accumulator
    }

    @Override
    public T getResult(T accumulator) {
        return accumulator;
    }

    @Override
    public T merge(T a, T b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) > 0 ? a : b;
    }
}

// 最小值聚合
class MinAggregate<T extends Comparable<T>> implements AggregateFunction<T, T, T> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public T createAccumulator() {
        return null;
    }

    @Override
    public void add(T value, T accumulator) {
    }

    @Override
    public T getResult(T accumulator) {
        return accumulator;
    }

    @Override
    public T merge(T a, T b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) < 0 ? a : b;
    }
}
