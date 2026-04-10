package com.kxj.streamingdataengine.core.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * 流数据记录
 * 支持事件时间和处理时间
 */
@Getter
@Builder
@ToString
public class StreamRecord<T> implements Serializable, Comparable<StreamRecord<T>> {

    private static final long serialVersionUID = 1L;

    /**
     * 记录唯一标识
     */
    private final String key;

    /**
     * 实际数据
     */
    private final T value;

    /**
     * 事件时间（业务时间）
     */
    private final long eventTime;

    /**
     * 处理时间（系统时间）
     */
    private final long processingTime;

    /**
     * 数据到达时间
     */
    private final long ingestionTime;

    /**
     * 扩展属性
     */
    private final Map<String, Object> attributes;

    /**
     * 数据分区
     */
    private final int partition;

    /**
     * 序列号（用于排序和去重）
     */
    private final long sequenceNumber;

    public StreamRecord(String key, T value, long eventTime, int partition, long sequenceNumber) {
        this(key, value, eventTime, System.currentTimeMillis(), System.currentTimeMillis(),
                Map.of(), partition, sequenceNumber);
    }

    public StreamRecord(String key, T value, long eventTime, long processingTime, long ingestionTime,
                        Map<String, Object> attributes, int partition, long sequenceNumber) {
        this.key = key;
        this.value = value;
        this.eventTime = eventTime;
        this.processingTime = processingTime;
        this.ingestionTime = ingestionTime;
        this.attributes = attributes != null ? Map.copyOf(attributes) : Map.of();
        this.partition = partition;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * 创建带有新事件时间的记录
     */
    public StreamRecord<T> withEventTime(long newEventTime) {
        return new StreamRecord<>(key, value, newEventTime, processingTime, ingestionTime,
                attributes, partition, sequenceNumber);
    }

    /**
     * 创建带有新值的记录
     */
    public <R> StreamRecord<R> withValue(R newValue) {
        return new StreamRecord<>(key, newValue, eventTime, processingTime, ingestionTime,
                attributes, partition, sequenceNumber);
    }

    @Override
    public int compareTo(StreamRecord<T> other) {
        int cmp = Long.compare(this.eventTime, other.eventTime);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    /**
     * 计算延迟（事件时间与处理时间的差）
     */
    public long getLatency() {
        return processingTime - eventTime;
    }
}
