package com.kxj.streamingdataengine.storage.lsm;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LSM-Tree存储引擎测试
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LSMTreeTest {

    private LSMTree<String, String> lsmTree;

    @BeforeEach
    void setUp() {
        lsmTree = new LSMTree<>();
        log.info("LSM-Tree测试开始");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (lsmTree != null) {
            lsmTree.close();
        }
        log.info("LSM-Tree测试结束");
    }

    @Test
    @Order(1)
    @DisplayName("基本CRUD操作")
    void testBasicCrud() {
        log.info("测试基本CRUD操作");

        // 写入
        lsmTree.put("key1", "value1");
        lsmTree.put("key2", "value2");
        lsmTree.put("key3", "value3");

        // 读取
        Optional<String> value1 = lsmTree.get("key1");
        assertTrue(value1.isPresent());
        assertEquals("value1", value1.get());
        log.info("读取key1成功: {}", value1.get());

        // 更新
        lsmTree.put("key1", "updated_value1");
        Optional<String> updated = lsmTree.get("key1");
        assertTrue(updated.isPresent());
        assertEquals("updated_value1", updated.get());
        log.info("更新key1成功: {}", updated.get());

        // 删除
        lsmTree.delete("key2");
        Optional<String> deleted = lsmTree.get("key2");
        assertTrue(deleted.isEmpty());
        log.info("删除key2成功");
    }

    @Test
    @Order(2)
    @DisplayName("范围查询")
    void testRangeQuery() {
        log.info("测试范围查询");

        // 写入有序数据
        for (int i = 0; i < 100; i++) {
            lsmTree.put(String.format("key%03d", i), "value" + i);
        }

        // 范围查询
        List<Map.Entry<String, String>> results = lsmTree.range("key010", "key020");
        assertEquals(11, results.size()); // 包含边界
        log.info("范围查询返回 {} 条记录", results.size());

        // 验证顺序
        for (int i = 0; i < results.size(); i++) {
            String expectedKey = String.format("key%03d", i + 10);
            assertEquals(expectedKey, results.get(i).getKey());
        }
    }

    @Test
    @Order(3)
    @DisplayName("批量写入性能测试")
    void testBulkWritePerformance() {
        log.info("测试批量写入性能");

        int count = 10000;
        Map<String, String> batch = new HashMap<>();
        for (int i = 0; i < count; i++) {
            batch.put("key" + i, "value" + i);
        }

        long start = System.currentTimeMillis();
        lsmTree.putAll(batch);
        long duration = System.currentTimeMillis() - start;

        double throughput = count * 1000.0 / duration;
        log.info("写入 {} 条记录，耗时 {} ms，吞吐量 {} 记录/秒",
                count, duration, String.format("%.2f", throughput));

        // 验证写入
        LSMTree.Stats stats = lsmTree.getStats();
        log.info("MemTable大小: {}, Segment数量: {}",
                stats.getActiveMemTableSize(), stats.getSegmentCount());

        assertTrue(stats.getActiveMemTableSize() > 0 || stats.getSegmentCount() > 0);
    }

    @Test
    @Order(4)
    @DisplayName("并发写入测试")
    void testConcurrentWrite() throws InterruptedException {
        log.info("测试并发写入");

        int threadCount = 10;
        int recordsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long start = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < recordsPerThread; i++) {
                        String key = "thread" + threadId + "_key" + i;
                        String value = "value" + i;
                        lsmTree.put(key, value);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - start;
        int totalRecords = threadCount * recordsPerThread;
        double throughput = totalRecords * 1000.0 / duration;

        log.info("并发写入完成: {} 线程 x {} 记录 = {} 总记录, 耗时 {} ms, 吞吐量 {} 记录/秒",
                threadCount, recordsPerThread, totalRecords, duration, String.format("%.2f", throughput));

        // 验证部分数据
        Optional<String> value = lsmTree.get("thread0_key0");
        assertTrue(value.isPresent());
        assertEquals("value0", value.get());
    }

    @Test
    @Order(5)
    @DisplayName("Compaction测试")
    void testCompaction() throws IOException, InterruptedException {
        log.info("测试Compaction");

        // 写入足够数据触发flush
        for (int i = 0; i < 100000; i++) {
            lsmTree.put("key" + i, "value" + i);
        }

        // 等待后台flush完成
        Thread.sleep(2000);

        LSMTree.Stats statsBefore = lsmTree.getStats();
        log.info("Compaction前 - MemTable: {}, Segments: {}, 总大小: {}",
                statsBefore.getActiveMemTableSize(),
                statsBefore.getSegmentCount(),
                statsBefore.getTotalDiskSize());

        // 手动触发compaction
        lsmTree.compact();

        LSMTree.Stats statsAfter = lsmTree.getStats();
        log.info("Compaction后 - MemTable: {}, Segments: {}, 总大小: {}",
                statsAfter.getActiveMemTableSize(),
                statsAfter.getSegmentCount(),
                statsAfter.getTotalDiskSize());

        // 验证数据完整性
        for (int i = 0; i < 100; i++) {
            Optional<String> value = lsmTree.get("key" + i);
            assertTrue(value.isPresent(), "Key" + i + "应该存在");
            assertEquals("value" + i, value.get());
        }
    }
}
