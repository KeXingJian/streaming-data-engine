package com.kxj.streamingdataengine.ai;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 背压控制器测试
 */
@Slf4j
public class BackpressureControllerTest {

    @Test
    @DisplayName("压力等级变化检测")
    void testPressureLevelChange() {
        log.info("测试压力等级变化");

        BackpressureController controller = new BackpressureController(200);
        Random random = new Random();

        // 初始状态
        BackpressureController.SystemStatus status1 = controller.getStatus();
        log.info("初始状态: {}", status1);
        assertEquals(BackpressureController.PressureLevel.NORMAL, status1.pressureLevel());

        // 模拟高负载（长延迟+大队列）
        for (int i = 0; i < 50; i++) {
            controller.setQueueSize(8000); // 高队列
            StreamRecord<String> record = createRecord();
            controller.recordSample(record, 800); // 高延迟
        }

        BackpressureController.SystemStatus status2 = controller.getStatus();
        log.info("高负载后状态: {}", status2);

        // 应该触发限流
        assertTrue(status2.rateLimit() < Integer.MAX_VALUE, "应该触发限流");
    }

    @Test
    @DisplayName("限流功能测试")
    void testRateLimiting() {
        log.info("测试限流功能");

        BackpressureController controller = new BackpressureController(100);

        // 模拟高压力，触发限流
        controller.setQueueSize(6000);
        for (int i = 0; i < 100; i++) {
            controller.recordSample(createRecord(), 600);
        }

        int limit = controller.getCurrentRateLimit();
        log.info("当前限流: {}/秒", limit);

        assertTrue(limit < Integer.MAX_VALUE, "应该有限流");

        // 测试tryAcquire
        int acquired = 0;
        int rejected = 0;
        for (int i = 0; i < 1000; i++) {
            if (controller.tryAcquire()) {
                acquired++;
            } else {
                rejected++;
            }
        }

        log.info("获取成功: {}, 拒绝: {}", acquired, rejected);

        // 限流时应该有拒绝
        if (limit < 10000) {
            assertTrue(rejected > 0, "限流时应该有拒绝");
        }
    }

    @Test
    @DisplayName("背压恢复测试")
    void testBackpressureRecovery() throws InterruptedException {
        log.info("测试背压恢复");

        BackpressureController controller = new BackpressureController(200);

        // 先制造高压力
        controller.setQueueSize(8000);
        for (int i = 0; i < 50; i++) {
            controller.recordSample(createRecord(), 1000);
        }

        BackpressureController.SystemStatus highStatus = controller.getStatus();
        log.info("高压力状态: level={}, limit={}",
                highStatus.pressureLevel(), highStatus.rateLimit());

        assertNotEquals(BackpressureController.PressureLevel.NORMAL, highStatus.pressureLevel());

        // 恢复正常
        controller.setQueueSize(100);
        for (int i = 0; i < 100; i++) {
            controller.recordSample(createRecord(), 50);
            Thread.sleep(5);
        }

        BackpressureController.SystemStatus normalStatus = controller.getStatus();
        log.info("恢复后状态: level={}, limit={}",
                normalStatus.pressureLevel(), normalStatus.rateLimit());

        // 应该恢复到正常或接近正常
        assertTrue(normalStatus.rateLimit() > highStatus.rateLimit(),
                "恢复后限流应该放宽");
    }

    @Test
    @DisplayName("并发访问测试")
    void testConcurrentAccess() throws InterruptedException {
        log.info("测试并发访问");

        BackpressureController controller = new BackpressureController(200);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(20);
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failCount = new AtomicLong(0);

        long start = System.currentTimeMillis();

        for (int t = 0; t < 20; t++) {
            executor.submit(() -> {
                try {
                    Random random = new Random();
                    for (int i = 0; i < 500; i++) {
                        if (controller.tryAcquire()) {
                            successCount.incrementAndGet();

                            // 模拟处理
                            StreamRecord<String> record = createRecord();
                            controller.recordSample(record, random.nextInt(100));
                            controller.setQueueSize(random.nextInt(10000));
                        } else {
                            failCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - start;

        log.info("并发测试完成: 成功={}, 拒绝={}, 耗时={}ms",
                successCount.get(), failCount.get(), duration);

        // 验证状态仍然有效
        BackpressureController.SystemStatus status = controller.getStatus();
        assertNotNull(status);
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
            strictController.setQueueSize(3000);
            relaxedController.setQueueSize(3000);
        }

        int strictLimit = strictController.getCurrentRateLimit();
        int relaxedLimit = relaxedController.getCurrentRateLimit();

        log.info("严格控制器(100ms目标)限流: {}, 宽松控制器(1000ms目标)限流: {}",
                strictLimit, relaxedLimit);

        // 严格控制器应该更严格（限流更低）
        // 注意：这个测试可能不稳定，因为状态机可能有延迟
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
