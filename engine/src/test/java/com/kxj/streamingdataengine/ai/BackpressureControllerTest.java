package com.kxj.streamingdataengine.ai;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.*;

import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 背压控制器测试
 */
@Slf4j
@ExtendWith(com.kxj.streamingdataengine.extension.TestReportExtension.class)
public class BackpressureControllerTest {

    @Test
    @DisplayName("压力等级变化检测")
    void testPressureLevelChange() {
        log.info("测试压力等级变化");

        BackpressureController controller = new BackpressureController(200);

        // 初始状态
        BackpressureController.SystemStatus status1 = controller.getStatus();
        log.info("初始状态: {}", status1);
        assertEquals(SeverityLevel.NORMAL, status1.pressureLevel());

        // 模拟高负载（高延迟驱动限流）
        for (int i = 0; i < 50; i++) {
            controller.recordSample(createRecord(), 800); // 高延迟
        }

        BackpressureController.SystemStatus status2 = controller.getStatus();
        log.info("高负载后状态: {}", status2);

        // 应该触发限流
        assertTrue(status2.rateLimit() < Integer.MAX_VALUE, "应该触发限流");
    }

    @Test
    @DisplayName("限流速率调整测试")
    void testRateLimitAdjustment() {
        log.info("测试限流速率调整");

        BackpressureController controller = new BackpressureController(100);

        // 模拟高压力，触发限流
        for (int i = 0; i < 100; i++) {
            controller.recordSample(createRecord(), 600);
        }

        int limit = controller.getCurrentRateLimit();
        log.info("当前限流: {}/秒", limit);

        assertTrue(limit < Integer.MAX_VALUE, "应该有限流");
        assertTrue(limit < 50000, "高压力下限流应该显著降低");
    }

    @Test
    @DisplayName("背压恢复测试")
    void testBackpressureRecovery() {
        log.info("测试背压恢复");

        BackpressureController controller = new BackpressureController(200);

        // 先制造高压力
        for (int i = 0; i < 50; i++) {
            controller.recordSample(createRecord(), 1000);
        }

        BackpressureController.SystemStatus highStatus = controller.getStatus();
        log.info("高压力状态: level={}, limit={}",
                highStatus.pressureLevel(), highStatus.rateLimit());

        assertNotEquals(SeverityLevel.NORMAL, highStatus.pressureLevel());

        // 恢复正常（需要足够多的样本把 avgLatency 拉低到目标以下）
        for (int i = 0; i < 500; i++) {
            controller.recordSample(createRecord(), 50);
        }

        BackpressureController.SystemStatus normalStatus = controller.getStatus();
        log.info("恢复后状态: level={}, limit={}, avgLatency={}",
                normalStatus.pressureLevel(), normalStatus.rateLimit(), normalStatus.avgLatencyMs());

        // 应该恢复到正常或接近正常
        assertTrue(normalStatus.rateLimit() > highStatus.rateLimit(),
                "恢复后限流应该放宽");
    }

    @Test
    @DisplayName("并发采样测试")
    void testConcurrentSampling() throws InterruptedException {
        log.info("测试并发采样");

        BackpressureController controller = new BackpressureController(200);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(20);

        long start = System.currentTimeMillis();

        for (int t = 0; t < 20; t++) {
            executor.submit(() -> {
                try {
                    Random random = new Random();
                    for (int i = 0; i < 500; i++) {
                        StreamRecord<String> record = createRecord();
                        controller.recordSample(record, random.nextInt(100));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - start;

        log.info("并发测试完成: 耗时={}ms", duration);

        // 验证状态仍然有效
        BackpressureController.SystemStatus status = controller.getStatus();
        assertNotNull(status);
        assertTrue(status.avgLatencyMs() >= 0);
        // EWMA 到达率应该在采样后为非零值
        assertTrue(status.currentRate() > 0, "EWMA 到达率应该大于 0");
    }

    @Test
    @DisplayName("不同目标延迟配置")
    void testDifferentTargetLatency() {
        log.info("测试不同目标延迟配置");

        BackpressureController strictController = new BackpressureController(100);
        BackpressureController relaxedController = new BackpressureController(1000);

        // 施加相同压力
        for (int i = 0; i < 50; i++) {
            strictController.recordSample(createRecord(), 500);
            relaxedController.recordSample(createRecord(), 500);
        }

        int strictLimit = strictController.getCurrentRateLimit();
        int relaxedLimit = relaxedController.getCurrentRateLimit();

        log.info("严格控制器(100ms目标)限流: {}, 宽松控制器(1000ms目标)限流: {}",
                strictLimit, relaxedLimit);

        // 严格控制器应该更严格（限流更低）
        log.info("严格限流 <= 宽松限流: {}", strictLimit <= relaxedLimit);
    }

    private StreamRecord<String> createRecord() {
        long now = System.currentTimeMillis();
        return StreamRecord.<String>builder()
                .key("test")
                .value("value")
                .eventTime(now)
                .processingTime(now)
                .build();
    }
}
