package com.kxj.streamingdataengine.stream;

import lombok.Data;

import java.time.Duration;

/**
 * 流处理配置
 */
@Data
public class StreamConfig {

    private int parallelism = Runtime.getRuntime().availableProcessors();         // 并行度，同时处理任务的线程数
    private long watermarkInterval = 200;                                         // Watermark生成间隔（毫秒）
    private boolean enableAdaptiveWindow = true;                                  // 是否启用自适应窗口动态调整
    private boolean enableBackpressure = true;                                    // 是否启用背压控制防止系统过载
    private int bufferSize = 10000;                                               // 缓冲区大小
    private int maxConcurrentOperators = 100;                                     // 最大并发算子数
    private Duration checkpointInterval = Duration.ofMinutes(1);                  // 检查点间隔
    private Duration maxOutOfOrderness = Duration.ofSeconds(5);                   // 最大乱序时间
    private Duration allowedLateness = Duration.ZERO;                             // 允许迟到时间
    private boolean autoWatermark = true;                                         // 自动水印策略
}
