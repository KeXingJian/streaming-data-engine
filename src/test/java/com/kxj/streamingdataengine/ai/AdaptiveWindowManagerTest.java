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
    @DisplayName("Little's Law + PID 方案与旧启发式方案对比证明")
    void testLittleLawPidVsLegacy() {
        log.info("[kxj: 自适应窗口算法对比测试 - Little's Law+PID vs 启发式规则]");

        long currentWindowSize = 10_000;

        // 场景A：低延迟 + 高吞吐
        // 旧方法：p95<100 -> x2.0, rate>10000 -> x1.5 => 30000
        // 新方法：Little's Law 预测 optimalSize = 2000 * 20000 / 1000 = 40000ms
        //         延迟远低于目标，PID 正向修正 => 显著 > Legacy
        AdaptiveWindowManager managerA = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long legacyA = managerA.predictOptimalWindowSizeLegacy(50, 80, 150, 20000, currentWindowSize);
        long v2A = managerA.predictOptimalWindowSizeV2(50, 80, 150, 20000, currentWindowSize);
        log.info("[kxj: 场景A 低延迟高吞吐] legacy={}, v2={}", legacyA, v2A);
        assertTrue(v2A > legacyA,
                "高吞吐场景下，Little's Law 应预测出更大的缓冲窗口");

        // 场景B：高延迟 + 低吞吐
        // 旧方法：p99>10000 -> x0.5, rate<100 -> x0.8 => 4000
        // 新方法：Little's Law 预测 optimalSize = 2000 * 10 / 1000 = 20ms
        //         延迟远超目标，PID 负向修正 => 快速收缩到最小
        AdaptiveWindowManager managerB = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long legacyB = managerB.predictOptimalWindowSizeLegacy(5000, 8000, 15000, 10, currentWindowSize);
        long v2B = managerB.predictOptimalWindowSizeV2(5000, 8000, 15000, 10, currentWindowSize);
        log.info("[kxj: 场景B 高延迟低吞吐] legacy={}, v2={}", legacyB, v2B);
        assertTrue(v2B < legacyB,
                "高延迟场景下，PID 应快速收缩窗口以降低等待时间");

        // 场景C：平滑性对比 — 延迟微小变化时，Legacy 阶梯不变，V2 连续变化
        AdaptiveWindowManager managerC1 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        AdaptiveWindowManager managerC2 = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long legacyC1 = managerC1.predictOptimalWindowSizeLegacy(1000, 3000, 5000, 5000, currentWindowSize);
        long legacyC2 = managerC2.predictOptimalWindowSizeLegacy(1100, 3000, 5000, 5000, currentWindowSize);
        long v2C1 = managerC1.predictOptimalWindowSizeV2(1000, 3000, 5000, 5000, currentWindowSize);
        long v2C2 = managerC2.predictOptimalWindowSizeV2(1100, 3000, 5000, 5000, currentWindowSize);
        log.info("[kxj: 场景C 平滑性对比] legacyC1={}, legacyC2={}, v2C1={}, v2C2={}",
                legacyC1, legacyC2, v2C1, v2C2);
        assertEquals(legacyC1, legacyC2,
                "旧方法在阈值未跨越时应输出相同的离散值");
        assertNotEquals(v2C1, v2C2,
                "新方法基于 PID 应在输入微小变化时产生平滑连续的输出");

        // 场景D：数学一致性 — 当延迟等于目标延迟且无乱序时，V2 应严格等于 Little's Law 理论值
        AdaptiveWindowManager managerD = new AdaptiveWindowManager(Duration.ofSeconds(10));
        long v2D = managerD.predictOptimalWindowSizeV2(2000, 2000, 2000, 5000, currentWindowSize);
        long expectedLittleLaw = 2000L * 5000 / 1000; // 10000
        log.info("[kxj: 场景D Little's Law 一致性] expected={}, actual={}", expectedLittleLaw, v2D);
        assertEquals(expectedLittleLaw, v2D,
                "延迟等于目标值时，PID 修正应为 0，预测值应严格等于 Little's Law 理论值");
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
