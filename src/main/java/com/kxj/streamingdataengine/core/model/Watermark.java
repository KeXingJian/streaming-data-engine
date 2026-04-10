package com.kxj.streamingdataengine.core.model;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Watermark - 用于处理乱序数据
 * 借鉴Flink的Watermark机制
 */
@Getter
@ToString
public class Watermark implements Serializable, Comparable<Watermark> {

    private static final long serialVersionUID = 1L;

    /**
     * 表示所有小于此时间戳的事件都已到达
     */
    private final long timestamp;

    /**
     * Watermark生成时间
     */
    private final long emissionTime;

    /**
     * 分区ID
     */
    private final int partitionId;

    public Watermark(long timestamp) {
        this(timestamp, -1);
    }

    public Watermark(long timestamp, int partitionId) {
        this.timestamp = timestamp;
        this.emissionTime = System.currentTimeMillis();
        this.partitionId = partitionId;
    }

    /**
     * 判断事件是否迟到（相对于此Watermark）
     */
    public boolean isLate(long eventTimestamp) {
        return eventTimestamp < this.timestamp;
    }

    /**
     * 计算允许的最大乱序时间
     */
    public long getAllowedLateness() {
        return emissionTime - timestamp;
    }

    @Override
    public int compareTo(Watermark other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

    /**
     * 获取最大Watermark（取两者较大值）
     */
    public Watermark max(Watermark other) {
        return this.timestamp >= other.timestamp ? this : other;
    }

    /**
     * 特殊Watermark：表示流结束
     */
    public static Watermark endOfStream() {
        return new Watermark(Long.MAX_VALUE);
    }

    /**
     * 特殊Watermark：初始Watermark
     */
    public static Watermark initial() {
        return new Watermark(Long.MIN_VALUE);
    }
}
