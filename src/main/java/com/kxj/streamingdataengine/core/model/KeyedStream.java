package com.kxj.streamingdataengine.core.model;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.window.Window;

import java.util.function.Function;

/**
 * 按键分区的流
 */
public interface KeyedStream<T, K> extends DataStream<T> {

    /**
     * 获取键提取器
     */
    Function<T, K> getKeyExtractor();

    /**
     * 按键聚合
     */
    <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction);

    /**
     * 按键滚动聚合
     */
    DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer);

    /**
     * 按键分组窗口
     */
    WindowedStream<T> window(Window.Assigner<T> assigner);
}
