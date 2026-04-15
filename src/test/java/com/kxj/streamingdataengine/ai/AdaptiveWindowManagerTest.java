package com.kxj.streamingdataengine.ai;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 自适应窗口管理器测试
 */
@Slf4j
public class AdaptiveWindowManagerTest {

    @Test
    @DisplayName("窗口大小自适应调整")
    void testAdaptiveWindowSize() throws InterruptedException {
        log.info("测试自适应窗口大小调整");

        AdaptiveWindowManager manager = new AdaptiveWindowManager(Duration.ofSeconds(10));

        Random random = new Random();

        // 模拟低延迟数据（预期窗口增大）
        log.info("阶段1: 模拟低延迟数据");
        for (int i = 0; i < 200; i++) {
            StreamRecord<String> record = createRecord(random.nextInt(50) + 50); // 50-100ms延迟
            manager.collectSample(record);
            Thread.sleep(10);
        }

        Duration windowSize1 = manager.getCurrentWindowSize();
        log.info("低延迟阶段后窗口大小: {}", windowSize1);

        // 模拟高延迟数据（预期窗口减小）
        log.info("阶段2: 模拟高延迟数据");
        for (int i = 0; i < 200; i++) {
            StreamRecord<String> record = createRecord(random.nextInt(500) + 500); // 500-1000ms延迟
            manager.collectSample(record);
            Thread.sleep(10);
        }

        Duration windowSize2 = manager.getCurrentWindowSize();
        log.info("高延迟阶段后窗口大小: {}", windowSize2);

        // 高延迟时窗口应该变小
        assertTrue(windowSize2.toMillis() <= windowSize1.toMillis(),
                "高延迟时窗口应该变小或保持不变");

        // 检查Watermark延迟
        long watermarkDelayMs = manager.getRecommendedWatermarkDelayMs();
        log.info("推荐Watermark延迟: {} ms", watermarkDelayMs);
        assertTrue(watermarkDelayMs > 0);
    }

    @Test
    @DisplayName("并发采样测试")
    void testConcurrentSampling() throws InterruptedException {
        log.info("测试并发采样");

        AdaptiveWindowManager manager = new AdaptiveWindowManager(Duration.ofSeconds(5));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);

        long start = System.currentTimeMillis();

        for (int t = 0; t < 10; t++) {
            executor.submit(() -> {
                try {
                    Random random = new Random();
                    for (int i = 0; i < 100; i++) {
                        StreamRecord<String> record = createRecord(random.nextInt(200));
                        manager.collectSample(record);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - start;
        log.info("并发采样完成，耗时 {} ms", duration);

        // 验证窗口仍然有效
        assertNotNull(manager.getCurrentWindowSize());
        assertTrue(manager.getCurrentWindowSize().toMillis() > 0);
    }

    @Test
    @DisplayName("窗口分配测试")
    void testWindowAssignment() {
        log.info("测试窗口分配");

        AdaptiveWindowManager manager = new AdaptiveWindowManager(Duration.ofSeconds(10));

        long timestamp = System.currentTimeMillis();
        var windows = manager.assignWindows(timestamp);

        assertFalse(windows.isEmpty());
        log.info("时间戳 {} 被分配到 {} 个窗口", timestamp, windows.size());

        // 验证窗口包含该时间戳
        windows.forEach(window -> {
            assertTrue(window.contains(timestamp),
                    "窗口应该包含分配的时间戳");
            log.info("窗口范围: {} - {}", window.getStart(), window.getEnd());
        });
    }

    @Test
    @DisplayName("Little's Law + PID 窗口预测核心特性验证")
    void testPredictOptimalWindowSize() {
        log.info("[kxj: 自适应窗口算法测试 - Little's Law + PID]");

        long currentWindowSize = 10_000;

        // 场景A：数学一致性 — 当延迟等于目标延迟且无乱序时，应严格等于 Little's Law 理论值
        AdaptiveWindowManager managerA = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long predictedA = managerA.predictOptimalWindowSize(2000, 2000, 2000, 5000, currentWindowSize);
        long expectedLittleLaw = 2000L * 5000 / 1000; // 10000
        log.info("[kxj: 场景A Little's Law 一致性] expected={}, actual={}", expectedLittleLaw, predictedA);
        assertEquals(expectedLittleLaw, predictedA,
                "延迟等于目标值时，PID 修正应为 0，预测值应严格等于 Little's Law 理论值");

        // 场景B：低延迟 + 高吞吐 — PID 正向修正，窗口应显著放大
        AdaptiveWindowManager managerB = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long predictedB = managerB.predictOptimalWindowSize(50, 80, 150, 20000, currentWindowSize);
        log.info("[kxj: 场景B 低延迟高吞吐] predicted={}", predictedB);
        assertTrue(predictedB > currentWindowSize,
                "低延迟高吞吐时，预测窗口应大于初始值");

        // 场景C：高延迟 + 低吞吐 — PID 负向修正，窗口应收缩
        AdaptiveWindowManager managerC = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long predictedC = managerC.predictOptimalWindowSize(5000, 8000, 15000, 10, currentWindowSize);
        log.info("[kxj: 场景C 高延迟低吞吐] predicted={}", predictedC);
        assertTrue(predictedC < currentWindowSize,
                "高延迟低吞吐时，预测窗口应小于初始值");

        // 场景D：PID 平滑性 — 延迟微小变化时，输出应连续变化而非阶梯跳跃
        AdaptiveWindowManager managerD1 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        AdaptiveWindowManager managerD2 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long predictedD1 = managerD1.predictOptimalWindowSize(1000, 3000, 5000, 5000, currentWindowSize);
        long predictedD2 = managerD2.predictOptimalWindowSize(1100, 3000, 5000, 5000, currentWindowSize);
        log.info("[kxj: 场景D PID平滑性] predictedD1={}, predictedD2={}", predictedD1, predictedD2);
        assertNotEquals(predictedD1, predictedD2,
                "PID 控制器应在输入微小变化时产生平滑连续的输出");

        // 场景E：乱序补偿 — disorderRatio > 3 时，预测值应比无乱序时更大
        AdaptiveWindowManager managerE1 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        AdaptiveWindowManager managerE2 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long normalE = managerE1.predictOptimalWindowSize(2000, 2100, 2200, 5000, currentWindowSize);
        long disorderE = managerE2.predictOptimalWindowSize(200, 1000, 3000, 5000, currentWindowSize); // disorderRatio = 15 > 3
        log.info("[kxj: 场景E 乱序补偿] normalE={}, disorderE={}", normalE, disorderE);
        assertTrue(disorderE > normalE,
                "乱序严重时，预测窗口应额外放大以等待更多迟到数据");
    }

    private StreamRecord<String> createRecord(long latency) {
        long now = System.currentTimeMillis();
        return StreamRecord.<String>builder()
                .key("test-key")
                .value("test-value")
                .eventTime(now - latency)
                .processingTime(now)
                .build();
    }
}
