package com.kxj.streamingdataengine.core.watermark;

import com.kxj.streamingdataengine.core.model.Watermark;

/**
 * Watermark 生成器
 * 根据事件序列推断当前水位线
 */
public interface WatermarkGenerator<T> {
    void onEvent(T event, long eventTimestamp);
    Watermark getCurrentWatermark();
}
