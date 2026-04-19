package com.kxj.streamingdataengine.core.watermark;

import com.kxj.streamingdataengine.core.model.Watermark;

/**
 * 升序时间戳 Watermark 生成器
 * 假设事件时间单调递增，无乱序
 */
public class AscendingTimestampsWatermarks<T> implements WatermarkGenerator<T> {

    private long maxTimestamp = Long.MIN_VALUE;

    @Override
    public void onEvent(T event, long eventTimestamp) {
        if (eventTimestamp > maxTimestamp) {
            maxTimestamp = eventTimestamp;
        }
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp);
    }
}
