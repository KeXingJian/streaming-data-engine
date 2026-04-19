package com.kxj.streamingdataengine.core.watermark;

/**
 * Watermark 策略
 * 组合 TimestampAssigner 与 WatermarkGenerator 的工厂
 */
public interface WatermarkStrategy<T> {
    TimestampAssigner<T> createTimestampAssigner();
    WatermarkGenerator<T> createWatermarkGenerator();
}
