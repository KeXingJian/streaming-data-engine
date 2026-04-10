package com.kxj.streamingdataengine.core.model;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 数据源接口
 */
public interface DataSource<T> extends Iterator<StreamRecord<T>>, Closeable {

    /**
     * 获取下一个记录
     */
    StreamRecord<T> nextRecord();

    /**
     * 是否还有更多数据
     */
    boolean hasMore();

    @Override
    default boolean hasNext() {
        return hasMore();
    }

    @Override
    default StreamRecord<T> next() {
        return nextRecord();
    }
}
