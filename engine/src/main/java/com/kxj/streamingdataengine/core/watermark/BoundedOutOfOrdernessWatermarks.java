package com.kxj.streamingdataengine.core.watermark;

import com.kxj.streamingdataengine.core.model.Watermark;

import java.time.Duration;

/**
 * 有界乱序 Watermark 生成器
 * 基于当前最大事件时间减去允许的最大乱序时间
 */
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    private final long maxOutOfOrderness;
    private long maxTimestamp = Long.MIN_VALUE;

    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness.toMillis();
    }

    @Override
    public void onEvent(T event, long eventTimestamp) {
        if (eventTimestamp > maxTimestamp) {
            maxTimestamp = eventTimestamp;
        }
    }

    @Override
    public Watermark getCurrentWatermark() {
        long timestamp = maxTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : maxTimestamp - maxOutOfOrderness;
        return new Watermark(timestamp);
    }
}
