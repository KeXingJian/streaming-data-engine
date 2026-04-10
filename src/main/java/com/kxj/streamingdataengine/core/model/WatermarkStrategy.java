package com.kxj.streamingdataengine.core.model;

/**
 * Watermark生成策略
 * 借鉴Flink的Watermark策略
 */
@FunctionalInterface
public interface WatermarkStrategy<T> {

    /**
     * 创建Watermark生成器
     */
    WatermarkGenerator<T> createWatermarkGenerator();

    /**
     * 获取时间戳提取器
     */
    default TimestampAssigner<T> createTimestampAssigner() {
        return (element, recordTimestamp) -> recordTimestamp;
    }

    /**
     * 固定延迟Watermark策略（允许乱序）
     */
    static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(long maxOutOfOrderness) {
        return () -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
    }

    /**
     * 单调递增Watermark策略（无乱序）
     */
    static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
        return () -> new AscendingTimestampsWatermarks<>();
    }

    /**
     * 固定间隔生成Watermark
     */
    static <T> WatermarkStrategy<T> forPeriodic(long intervalMillis) {
        return () -> new PeriodicWatermarkGenerator<>(intervalMillis);
    }

    /**
     * 带时间戳提取的Watermark策略
     */
    default WatermarkStrategy<T> withIdleness(long idleTimeout) {
        return new WatermarkStrategy<>() {
            @Override
            public WatermarkGenerator<T> createWatermarkGenerator() {
                return WatermarkStrategy.this.createWatermarkGenerator();
            }

            @Override
            public TimestampAssigner<T> createTimestampAssigner() {
                return WatermarkStrategy.this.createTimestampAssigner();
            }
        };
    }
}

/**
 * 时间戳提取器
 */
@FunctionalInterface
interface TimestampAssigner<T> {
    long extractTimestamp(T element, long recordTimestamp);
}

/**
 * Watermark生成器接口
 */
interface WatermarkGenerator<T> {
    /**
     * 每个事件到达时调用
     */
    void onEvent(T event, long eventTimestamp, long currentWatermark);

    /**
     * 周期性调用生成Watermark
     */
    Watermark getCurrentWatermark();

    /**
     * 标记流结束
     */
    default Watermark onPeriodicEmit() {
        return getCurrentWatermark();
    }
}

/**
 * 有界乱序Watermark生成器
 */
class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {
    private final long maxOutOfOrderness;
    private long maxTimestamp = Long.MIN_VALUE + 1;

    public BoundedOutOfOrdernessWatermarks(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, long currentWatermark) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp - maxOutOfOrderness - 1);
    }
}

/**
 * 单调递增Watermark生成器
 */
class AscendingTimestampsWatermarks<T> implements WatermarkGenerator<T> {
    private long maxTimestamp = Long.MIN_VALUE + 1;

    @Override
    public void onEvent(T event, long eventTimestamp, long currentWatermark) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp - 1);
    }
}

/**
 * 周期性Watermark生成器
 */
class PeriodicWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private final long interval;
    private long maxTimestamp = Long.MIN_VALUE + 1;
    private long lastEmittedWatermark = Long.MIN_VALUE;

    public PeriodicWatermarkGenerator(long intervalMillis) {
        this.interval = intervalMillis;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, long currentWatermark) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public Watermark getCurrentWatermark() {
        long now = System.currentTimeMillis();
        if (now - lastEmittedWatermark >= interval) {
            lastEmittedWatermark = now;
            return new Watermark(maxTimestamp);
        }
        return null;
    }
}
