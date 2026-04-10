package com.kxj.streamingdataengine.core.model;

import java.io.Closeable;

/**
 * 数据输出接口
 */
public interface DataSink<T> extends Closeable {

    /**
     * 写入数据
     */
    void write(T value) throws Exception;

    /**
     * 刷新数据
     */
    default void flush() throws Exception {}

    /**
     * 初始化
     */
    default void open() throws Exception {}

    @Override
    default void close() {}
}
