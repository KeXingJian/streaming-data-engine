package com.kxj.streamingdataengine.ai;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异常检测器测试
 */
@Slf4j
public class AnomalyDetectorTest {

    @Test
    @DisplayName("正常流量不应触发异常")
    void testNormalTraffic() {
        log.info("测试正常流量");

        AtomicInteger alertCount = new AtomicInteger(0);
        AnomalyDetector detector = new AnomalyDetector(result -> {
            alertCount.incrementAndGet();
            log.info("异常告警: {}", result);
        });

        Random random = new Random();

        // 发送稳定的正常流量
        for (int i = 0; i < 100; i++) {
            detector.recordSample(100 + random.nextGaussian() * 10);
        }

        // 正常流量不应触发太多告警（可能偶发）
        log.info("正常流量触发告警次数: {}", alertCount.get());
        assertTrue(alertCount.get() <= 5, "正常流量不应频繁触发告警");
    }

    @Test
    @DisplayName("突发流量应触发异常")
    void testSuddenSpike() throws InterruptedException {
        log.info("测试突发流量检测");

        List<AnomalyDetector.AnomalyResult> alerts = new ArrayList<>();
        AnomalyDetector detector = new AnomalyDetector(alerts::add);

        Random random = new Random();

        // 建立基线
        log.info("建立基线...");
        for (int i = 0; i < 50; i++) {
            detector.recordSample(100 + random.nextGaussian() * 10);
            Thread.sleep(10);
        }

        // 突发流量
        log.info("模拟突发流量...");
        for (int i = 0; i < 20; i++) {
            detector.recordSample(300 + random.nextGaussian() * 20); // 3倍正常流量
            Thread.sleep(10);
        }

        // 恢复
        log.info("恢复正常流量...");
        for (int i = 0; i < 30; i++) {
            detector.recordSample(100 + random.nextGaussian() * 10);
            Thread.sleep(10);
        }

        log.info("触发告警次数: {}", alerts.size());
        alerts.forEach(alert ->
                log.info("告警级别: {}, 当前值: {}, 基线: {}",
                        alert.getLevel(),
                        String.format("%.2f", alert.getCurrentValue()),
                        String.format("%.2f", alert.getBaseline())));

        // 应该检测到异常
        assertFalse(alerts.isEmpty(), "应该检测到异常");

        // 应该有高级别告警
        boolean hasHighLevelAlert = alerts.stream()
                .anyMatch(a -> a.getLevel().ordinal() >= AnomalyDetector.AnomalyLevel.MEDIUM.ordinal());
        assertTrue(hasHighLevelAlert, "应该有中高级别告警");
    }

    @Test
    @DisplayName("流量下降检测")
    void testSuddenDrop() throws InterruptedException {
        log.info("测试流量下降检测");

        List<AnomalyDetector.AnomalyResult> alerts = new ArrayList<>();
        AnomalyDetector detector = new AnomalyDetector(alerts::add);

        Random random = new Random();

        // 建立高流量基线
        for (int i = 0; i < 50; i++) {
            detector.recordSample(500 + random.nextGaussian() * 20);
            Thread.sleep(10);
        }

        // 突然下降
        for (int i = 0; i < 20; i++) {
            detector.recordSample(50 + random.nextGaussian() * 5);
            Thread.sleep(10);
        }

        log.info("流量下降触发告警: {}", alerts.size());

        // 应该检测到异常
        assertFalse(alerts.isEmpty(), "流量骤降也应该触发告警");
    }

    @Test
    @DisplayName("统计信息准确性")
    void testStatisticsAccuracy() {
        log.info("测试统计信息准确性");

        AnomalyDetector detector = new AnomalyDetector(r -> {});

        // 发送已知分布的数据
        for (int i = 0; i < 100; i++) {
            detector.recordSample(100.0);
        }

        AnomalyDetector.TrafficStatistics stats = detector.getStatistics();

        log.info("统计信息: mean={}, min={}, max={}, count={}",
                String.format("%.2f", stats.getMean()),
                String.format("%.2f", stats.getMin()),
                String.format("%.2f", stats.getMax()),
                stats.getCount());

        assertEquals(100.0, stats.getMean(), 0.01, "平均值应该是100");
        assertEquals(100.0, stats.getMin(), 0.01, "最小值应该是100");
        assertEquals(100.0, stats.getMax(), 0.01, "最大值应该是100");
        assertEquals(100, stats.getCount());
    }

    @Test
    @DisplayName("敏感度配置测试")
    void testSensitivityConfig() throws InterruptedException {
        log.info("测试不同敏感度配置");

        // 高敏感度检测器
        List<AnomalyDetector.AnomalyResult> sensitiveAlerts = new ArrayList<>();
        AnomalyDetector sensitiveDetector = new AnomalyDetector(
                AnomalyDetector.DetectionConfig.sensitiveConfig(),
                sensitiveAlerts::add
        );

        // 默认敏感度检测器
        List<AnomalyDetector.AnomalyResult> defaultAlerts = new ArrayList<>();
        AnomalyDetector defaultDetector = new AnomalyDetector(defaultAlerts::add);

        Random random = new Random();

        // 建立基线
        for (int i = 0; i < 50; i++) {
            double value = 100 + random.nextGaussian() * 5;
            sensitiveDetector.recordSample(value);
            defaultDetector.recordSample(value);
        }

        // 轻微异常
        for (int i = 0; i < 10; i++) {
            double value = 160 + random.nextGaussian() * 5; // 60%增长
            sensitiveDetector.recordSample(value);
            defaultDetector.recordSample(value);
            Thread.sleep(10);
        }

        log.info("高敏感度告警数: {}, 默认敏感度告警数: {}",
                sensitiveAlerts.size(), defaultAlerts.size());

        // 高敏感度应该检测更多异常
        assertTrue(sensitiveAlerts.size() >= defaultAlerts.size(),
                "高敏感度应该检测更多异常或相等");
    }
}
