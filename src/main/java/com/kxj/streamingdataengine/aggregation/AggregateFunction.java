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

    @Serial
    long serialVersionUID = 1L;

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
