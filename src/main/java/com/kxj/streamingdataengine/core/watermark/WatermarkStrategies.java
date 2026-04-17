package com.kxj.streamingdataengine.core.watermark;

import java.time.Duration;

/**
 * 常用 WatermarkStrategy 工厂方法
 */
public class WatermarkStrategies {

    /**
     * 基于有界乱序时间的策略
     */
    public static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness, TimestampAssigner<T> timestampAssigner) {
        return new WatermarkStrategy<>() {
            @Override
            public TimestampAssigner<T> createTimestampAssigner() {
                return timestampAssigner;
            }

            @Override
            public WatermarkGenerator<T> createWatermarkGenerator() {
                return new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
            }
        };
    }

    /**
     * 升序时间戳策略（无乱序）
     */
    public static <T> WatermarkStrategy<T> forMonotonousTimestamps(TimestampAssigner<T> timestampAssigner) {
        return new WatermarkStrategy<>() {
            @Override
            public TimestampAssigner<T> createTimestampAssigner() {
                return timestampAssigner;
            }

            @Override
            public WatermarkGenerator<T> createWatermarkGenerator() {
                return new AscendingTimestampsWatermarks<>();
            }
        };
    }
}
