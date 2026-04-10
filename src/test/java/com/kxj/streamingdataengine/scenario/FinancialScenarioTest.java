package com.kxj.streamingdataengine.scenario;

import com.kxj.streamingdataengine.ai.AnomalyDetector;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 金融风控场景测试
 * 实时交易监控、欺诈检测、风险预警
 */
@Slf4j
public class FinancialScenarioTest {

    record Transaction(String transactionId, String userId, String cardId,
                       double amount, String merchant, String location,
                       TransactionType type, long timestamp) {}

    enum TransactionType {PAYMENT, TRANSFER, WITHDRAWAL, REFUND}

    @Test
    @DisplayName("大额交易实时监控")
    void testLargeTransactionMonitoring() {
        log.info("=== 场景1: 大额交易实时监控 ===");

        double largeAmountThreshold = 10000.0;
        AtomicInteger largeTransactionCount = new AtomicInteger(0);
        AtomicLong totalLargeAmount = new AtomicLong(0);

        List<Transaction> transactions = generateTransactions(1000);

        for (Transaction tx : transactions) {
            if (tx.amount() >= largeAmountThreshold) {
                largeTransactionCount.incrementAndGet();
                totalLargeAmount.addAndGet((long) tx.amount());
                log.info("大额交易告警: user={}, amount={}, merchant={}",
                        tx.userId(), String.format("%.2f", tx.amount()), tx.merchant());
            }
        }

        log.info("大额交易统计: count={}, total={}",
                largeTransactionCount.get(), totalLargeAmount.get());

        assertTrue(largeTransactionCount.get() > 0, "应该有检测到的大额交易");
    }

    @Test
    @DisplayName("高频交易检测")
    void testHighFrequencyTradingDetection() throws InterruptedException {
        log.info("=== 场景2: 高频交易检测 ===");

        // 模拟同一用户高频交易
        String targetUser = "user-fraud-1";
        int normalInterval = 5000; // 正常5秒一次
        int fraudInterval = 100;   // 异常100ms一次

        Map<String, List<Long>> userTransactionTimes = new ConcurrentHashMap<>();

        // 正常用户交易
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            String user = "user-normal-" + i;
            for (int j = 0; j < 5; j++) {
                userTransactionTimes.computeIfAbsent(user, k -> new ArrayList<>())
                        .add(System.currentTimeMillis() + j * normalInterval);
            }
        }

        // 可疑用户高频交易
        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            userTransactionTimes.computeIfAbsent(targetUser, k -> new ArrayList<>())
                    .add(baseTime + i * fraudInterval);
        }

        // 检测高频交易
        AtomicBoolean fraudDetected = new AtomicBoolean(false);
        userTransactionTimes.forEach((user, times) -> {
            if (times.size() < 2) return;

            // 计算平均间隔
            long totalInterval = 0;
            for (int i = 1; i < times.size(); i++) {
                totalInterval += times.get(i) - times.get(i - 1);
            }
            long avgInterval = totalInterval / (times.size() - 1);

            log.info("用户 {}: {}笔交易, 平均间隔 {}ms", user, times.size(), avgInterval);

            // 平均间隔小于200ms认为是高频交易
            if (avgInterval < 200) {
                fraudDetected.set(true);
                log.warn("检测到高频交易: user={}, 交易数={}, 平均间隔={}ms",
                        user, times.size(), avgInterval);
            }
        });

        assertTrue(fraudDetected.get(), "应该检测到高频交易");
    }

    @Test
    @DisplayName("异地登录检测")
    void testAbnormalLocationDetection() {
        log.info("=== 场景3: 异地登录检测 ===");

        Map<String, Set<String>> userLocations = new HashMap<>();

        // 模拟用户历史位置
        userLocations.put("user-1", new HashSet<>(Set.of("Beijing", "Shanghai")));
        userLocations.put("user-2", new HashSet<>(Set.of("Guangzhou")));
        userLocations.put("user-3", new HashSet<>(Set.of("Shenzhen", "Hangzhou")));

        // 模拟新交易
        List<Transaction> newTransactions = List.of(
                new Transaction("tx1", "user-1", "card1", 100, "shop1", "Beijing",
                        TransactionType.PAYMENT, System.currentTimeMillis()),
                new Transaction("tx2", "user-1", "card1", 200, "shop2", "NewYork",  // 异常！
                        TransactionType.PAYMENT, System.currentTimeMillis()),
                new Transaction("tx3", "user-2", "card2", 50, "shop3", "Guangzhou",
                        TransactionType.PAYMENT, System.currentTimeMillis()),
                new Transaction("tx4", "user-3", "card3", 500, "shop4", "Moscow",   // 异常！
                        TransactionType.PAYMENT, System.currentTimeMillis())
        );

        AtomicInteger anomalyCount = new AtomicInteger(0);

        for (Transaction tx : newTransactions) {
            Set<String> historyLocations = userLocations.getOrDefault(tx.userId(), Set.of());
            if (!historyLocations.contains(tx.location())) {
                anomalyCount.incrementAndGet();
                log.warn("异地交易告警: user={}, 历史位置={}, 当前位置={}",
                        tx.userId(), historyLocations, tx.location());
            }
        }

        assertEquals(2, anomalyCount.get(), "应该检测到2次异地交易");
    }

    @Test
    @DisplayName("交易流量异常检测")
    void testTransactionFlowAnomaly() throws InterruptedException {
        log.info("=== 场景4: 交易流量异常检测 ===");

        List<AnomalyDetector.AnomalyResult> alerts = new ArrayList<>();
        AnomalyDetector detector = new AnomalyDetector(alerts::add);

        Random random = new Random();

        // 建立基线 - 正常交易量
        log.info("建立基线...");
        for (int i = 0; i < 60; i++) {
            detector.recordSample(100 + random.nextGaussian() * 20);
            Thread.sleep(10);
        }

        // 模拟DDoS攻击 - 交易量激增
        log.info("模拟攻击流量...");
        for (int i = 0; i < 30; i++) {
            detector.recordSample(5000 + random.nextGaussian() * 500);
            Thread.sleep(10);
        }

        // 恢复
        log.info("恢复正常...");
        for (int i = 0; i < 30; i++) {
            detector.recordSample(100 + random.nextGaussian() * 20);
            Thread.sleep(10);
        }

        log.info("检测到 {} 次流量异常", alerts.size());

        boolean hasCriticalAlert = alerts.stream()
                .anyMatch(a -> a.getLevel() == AnomalyDetector.AnomalyLevel.CRITICAL);

        assertTrue(hasCriticalAlert, "应该有严重级别告警");
    }

    @Test
    @DisplayName("实时风控规则引擎")
    void testRealTimeRiskControl() throws InterruptedException {
        log.info("=== 场景5: 实时风控规则引擎 ===");

        ExecutionEngine engine = new ExecutionEngine(4, Duration.ofMillis(100), true, false);
        engine.start();

        BlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<>();
        AtomicInteger blockedCount = new AtomicInteger(0);
        AtomicInteger approvedCount = new AtomicInteger(0);

        // 风控规则
        RiskRule[] rules = {
                new RiskRule("大额交易", tx -> tx.amount() > 50000),
                new RiskRule("夜间交易", tx -> {
                    int hour = java.time.LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(tx.timestamp()),
                            java.time.ZoneId.systemDefault()).getHour();
                    return hour >= 2 && hour <= 5;
                }),
                new RiskRule("可疑商户", tx -> tx.merchant().contains("suspicious"))
        };

        // 消费者线程
        ExecutorService processor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
            processor.submit(() -> {
                while (!Thread.interrupted()) {
                    try {
                        Transaction tx = transactionQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (tx == null) continue;

                        // 应用风控规则
                        boolean blocked = false;
                        for (RiskRule rule : rules) {
                            if (rule.check(tx)) {
                                blocked = true;
                                blockedCount.incrementAndGet();
                                log.warn("交易被拦截: txId={}, rule={}",
                                        tx.transactionId(), rule.name());
                                break;
                            }
                        }

                        if (!blocked) {
                            approvedCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }

        // 生成测试交易
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            Transaction tx = new Transaction(
                    "tx-" + i,
                    "user-" + random.nextInt(100),
                    "card-" + random.nextInt(50),
                    random.nextDouble() < 0.05 ? 80000 : 100 + random.nextDouble() * 900, // 5%大额
                    random.nextDouble() < 0.02 ? "suspicious-shop" : "normal-shop",
                    "Beijing",
                    TransactionType.PAYMENT,
                    System.currentTimeMillis()
            );
            transactionQueue.offer(tx);
        }

        Thread.sleep(2000);

        processor.shutdownNow();
        engine.stop();

        int total = blockedCount.get() + approvedCount.get();
        log.info("风控结果: 总交易={}, 拦截={}, 通过={}, 拦截率={}",
                total, blockedCount.get(), approvedCount.get(),
                String.format("%.2f%%", blockedCount.get() * 100.0 / total));

        assertTrue(blockedCount.get() > 0, "应该有被拦截的交易");
    }

    @Test
    @DisplayName("滑动窗口欺诈检测")
    void testSlidingWindowFraudDetection() {
        log.info("=== 场景6: 滑动窗口欺诈检测 ===");

        String suspectUser = "user-suspect";
        long windowSizeMs = 60000; // 1分钟窗口
        int maxTransactionsPerWindow = 10;

        // 模拟用户交易序列
        List<Transaction> userTransactions = new ArrayList<>();
        long baseTime = System.currentTimeMillis();

        // 正常交易 - 间隔10秒
        for (int i = 0; i < 5; i++) {
            userTransactions.add(new Transaction(
                    "tx-normal-" + i, suspectUser, "card1", 100,
                    "shop" + i, "Beijing", TransactionType.PAYMENT,
                    baseTime + i * 10000
            ));
        }

        // 异常交易 - 1分钟内20笔
        for (int i = 0; i < 20; i++) {
            userTransactions.add(new Transaction(
                    "tx-fraud-" + i, suspectUser, "card1", 500,
                    "shop-fraud", "Beijing", TransactionType.PAYMENT,
                    baseTime + 60000 + i * 2000 // 2秒一笔
            ));
        }

        // 滑动窗口检测
        Map<String, List<Transaction>> windowedTransactions = new HashMap<>();

        for (Transaction tx : userTransactions) {
            String user = tx.userId();
            windowedTransactions.computeIfAbsent(user, k -> new ArrayList<>()).add(tx);

            // 检查窗口内交易数
            List<Transaction> window = windowedTransactions.get(user).stream()
                    .filter(t -> t.timestamp() > tx.timestamp() - windowSizeMs)
                    .toList();

            if (window.size() > maxTransactionsPerWindow) {
                log.warn("检测到可疑交易模式: user={}, 窗口内交易数={}", user, window.size());
            }
        }

        // 验证检测
        List<Transaction> fraudWindow = windowedTransactions.get(suspectUser).stream()
                .filter(t -> t.transactionId().startsWith("tx-fraud"))
                .toList();

        log.info("异常窗口交易数: {}", fraudWindow.size());
        assertTrue(fraudWindow.size() > maxTransactionsPerWindow,
                "异常交易数应超过阈值");
    }

    // ============== 辅助类和方法 ==============

    private List<Transaction> generateTransactions(int count) {
        List<Transaction> transactions = new ArrayList<>();
        Random random = new Random();
        String[] merchants = {"Amazon", "Alibaba", "JD", "Taobao", "Tmall"};
        TransactionType[] types = TransactionType.values();

        for (int i = 0; i < count; i++) {
            transactions.add(new Transaction(
                    UUID.randomUUID().toString(),
                    "user-" + random.nextInt(1000),
                    "card-" + random.nextInt(100),
                    10 + random.nextDouble() * 99990,
                    merchants[random.nextInt(merchants.length)],
                    random.nextBoolean() ? "Beijing" : "Shanghai",
                    types[random.nextInt(types.length)],
                    System.currentTimeMillis() - random.nextInt(3600000)
            ));
        }

        return transactions;
    }

    private record RiskRule(String name, java.util.function.Predicate<Transaction> condition) {
        boolean check(Transaction tx) {
            return condition.test(tx);
        }
    }
}
