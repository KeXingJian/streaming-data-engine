package com.kxj.streamingdataengine.core.model;

import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;

import java.util.List;
import java.util.function.Function;

/**
 * 数据流抽象
 * 支持链式操作
 */
public interface DataStream<T> {

    /**
     * 获取流的唯一标识
     */
    String getId();

    /**
     * 获取流的分区数
     */
    int getParallelism();

    /**
     * 数据转换：map操作
     */
    <R> DataStream<R> map(Function<T, R> mapper);

    /**
     * 数据过滤
     */
    DataStream<T> filter(java.util.function.Predicate<T> predicate);

    /**
     * 按键分区
     */
    <K> KeyedStream<T, K> keyBy(Function<T, K> keyExtractor);

    /**
     * 分配时间窗口
     * @param assigner 窗口分配器
     */
    WindowedStream<T> window(Window.Assigner<T> assigner);

    /**
     * 添加算子
     */
    DataStream<T> addOperator(StreamOperator<T> operator);

    /**
     * 设置Watermark生成策略
     */
    DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> strategy);

    /**
     * 添加sink
     */
    DataStream<T> addSink(DataSink<T> sink);

    /**
     * 获取数据源
     */
    List<T> collect();

    /**
     * 执行流处理
     */
    void execute();

    /**
     * 执行并获取结果
     */
    StreamResult executeAndGet();
}
