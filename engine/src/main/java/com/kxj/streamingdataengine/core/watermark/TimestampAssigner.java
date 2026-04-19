package com.kxj.streamingdataengine.core.watermark;

/**
 * 从元素中提取事件时间戳
 */
@FunctionalInterface
public interface TimestampAssigner<T> {
    long extractTimestamp(T element);
}
