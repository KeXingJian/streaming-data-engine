package com.kxj.streamingdataengine.core.model;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.window.trigger.Trigger;

import java.util.function.Function;

/**
 * 窗口化流
 */
public interface WindowedStream<T> extends DataStream<T> {

    /**
     * 设置窗口触发器
     */
    WindowedStream<T> trigger(Trigger<T> trigger);

    /**
     * 设置允许迟到时间
     */
    WindowedStream<T> allowedLateness(long lateness);

    /**
     * 设置侧边输出标签（处理迟到数据）
     */
    WindowedStream<T> sideOutputLateData(String tag);

    /**
     * 窗口聚合
     */
    <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction);

    /**
     * 窗口聚合（带窗口结果转换）
     */
    <ACC, R, W> DataStream<W> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction,
            Function<R, W> windowResultMapper);

    /**
     * 窗口reduce操作
     */
    DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer);

    /**
     * 窗口fold操作
     */
    <R> DataStream<R> fold(R initialValue, java.util.function.BiFunction<R, T, R> folder);

    /**
     * 应用窗口函数
     */
    <R> DataStream<R> apply(Function<Iterable<T>, R> windowFunction);
}
