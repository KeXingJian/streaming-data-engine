package com.kxj.streamingdataengine.stream;

import lombok.Data;

import java.time.Duration;

/**
 * 流处理配置
 */
@Data
public class StreamConfig {

    /**
     * 并行度
     */
    private int parallelism = Runtime.getRuntime().availableProcessors();

    /**
     * Watermark生成间隔
     */
    private long watermarkInterval = 200;

    /**
     * 是否启用自适应窗口
     */
    private boolean enableAdaptiveWindow = true;

    /**
     * 是否启用背压控制
     */
    private boolean enableBackpressure = true;

    /**
     * 缓冲区大小
     */
    private int bufferSize = 10000;

    /**
     * 最大并发算子数
     */
    private int maxConcurrentOperators = 100;

    /**
     * 检查点间隔
     */
    private Duration checkpointInterval = Duration.ofMinutes(1);

    /**
     * 最大乱序时间
     */
    private Duration maxOutOfOrderness = Duration.ofSeconds(5);

    /**
     * 允许迟到时间
     */
    private Duration allowedLateness = Duration.ZERO;

    /**
     * 自动水印策略
     */
    private boolean autoWatermark = true;
}
