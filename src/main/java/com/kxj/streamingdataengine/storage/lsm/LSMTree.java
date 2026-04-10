package com.kxj.streamingdataengine.storage.lsm;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM-Tree (Log-Structured Merge-Tree)
 * 借鉴Kafka的日志结构设计
 *
 * 核心思想：
 * 1. 写入先进入MemTable（内存有序结构）
 * 2. MemTable满后刷入磁盘（不可变的Segment）
 * 3. 后台合并压缩（Compaction）
 * 4. 查询时先查MemTable，再查Segments
 */
@Slf4j
public class LSMTree<K extends Comparable<K>, V> {

    /**
     * 内存表配置
     */
    private final int memTableSize;
    private final int maxSegmentCount;

    /**
     * 当前活跃MemTable
     */
    private volatile MemTable<K, V> activeMemTable;

    /**
     * 只读MemTable（等待刷盘）
     */
    private final List<MemTable<K, V>> immutableMemTables;

    /**
     * 磁盘Segment
     */
    private final List<Segment<K, V>> segments;

    /**
     * 全局序列号生成器
     */
    private final AtomicLong sequenceNumber;

    /**
     * 读写锁
     */
    private final ReadWriteLock lock;

    /**
     * Compaction策略
     */
    private final CompactionStrategy compactionStrategy;

    /**
     * WAL（预写日志）
     */
    private final WriteAheadLog wal;

    public LSMTree() {
        this(64 * 1024 * 1024, 10, new SizeTieredCompaction(), null); // 64MB默认
    }

    public LSMTree(int memTableSize, int maxSegmentCount,
                   CompactionStrategy compactionStrategy, WriteAheadLog wal) {
        this.memTableSize = memTableSize;
        this.maxSegmentCount = maxSegmentCount;
        this.compactionStrategy = compactionStrategy;
        this.wal = wal;
        this.activeMemTable = new MemTable<>();
        this.immutableMemTables = Collections.synchronizedList(new ArrayList<>());
        this.segments = Collections.synchronizedList(new ArrayList<>());
        this.sequenceNumber = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * 写入数据
     */
    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            // [kxj: LSM-Tree写入流程 - 先写WAL保证持久化，再写MemTable]
            // 写入WAL
            if (wal != null) {
                wal.append(key, value);
            }

            // 写入MemTable
            long seq = sequenceNumber.incrementAndGet();
            activeMemTable.put(key, value, seq);

            // [kxj: MemTable达到阈值时触发刷盘，生成不可变Segment]
            if (activeMemTable.size() >= memTableSize) {
                log.debug("[kxj: MemTable刷盘] 当前大小={}", activeMemTable.size());
                flushMemTable();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 批量写入
     */
    public void putAll(Map<K, V> entries) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<K, V> entry : entries.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 查询数据
     */
    public Optional<V> get(K key) {
        lock.readLock().lock();
        try {
            // [kxj: LSM-Tree查询顺序 - 活跃MemTable → 不可变MemTable → 磁盘Segment]
            // 1. 查活跃MemTable
            Optional<Entry<V>> entry = activeMemTable.get(key);
            if (entry.isPresent()) {
                return entry.get().isDeleted() ? Optional.empty() : Optional.of(entry.get().getValue());
            }

            // 2. 查不可变MemTable（从新到旧）
            for (MemTable<K, V> memTable : immutableMemTables) {
                entry = memTable.get(key);
                if (entry.isPresent()) {
                    return entry.get().isDeleted() ? Optional.empty() : Optional.of(entry.get().getValue());
                }
            }

            // 3. 查磁盘Segment（从新到旧）
            for (Segment<K, V> segment : segments) {
                entry = segment.get(key);
                if (entry.isPresent()) {
                    return entry.get().isDeleted() ? Optional.empty() : Optional.of(entry.get().getValue());
                }
            }

            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 范围查询
     */
    public List<Map.Entry<K, V>> range(K startKey, K endKey) {
        lock.readLock().lock();
        try {
            Map<K, Entry<V>> result = new TreeMap<>();

            // 从旧到新查询，新数据覆盖旧数据
            for (Segment<K, V> segment : segments) {
                segment.range(startKey, endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
            }

            for (MemTable<K, V> memTable : immutableMemTables) {
                memTable.range(startKey, endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
            }

            activeMemTable.range(startKey, endKey).forEach(e -> result.put(e.getKey(), e.getValue()));

            // 过滤删除的数据
            List<Map.Entry<K, V>> filtered = new ArrayList<>();
            for (Map.Entry<K, Entry<V>> e : result.entrySet()) {
                if (!e.getValue().isDeleted()) {
                    filtered.add(Map.entry(e.getKey(), e.getValue().getValue()));
                }
            }

            return filtered;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 删除数据
     */
    public void delete(K key) {
        lock.writeLock().lock();
        try {
            if (wal != null) {
                wal.appendDelete(key);
            }
            activeMemTable.delete(key, sequenceNumber.incrementAndGet());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 刷盘MemTable
     */
    private void flushMemTable() {
        immutableMemTables.add(0, activeMemTable);
        activeMemTable = new MemTable<>();

        // 异步刷盘
        Thread.ofVirtual().start(() -> {
            try {
                flushToDisk();
            } catch (IOException e) {
                log.error("Flush to disk failed", e);
            }
        });
    }

    /**
     * 写入磁盘
     */
    private void flushToDisk() throws IOException {
        lock.writeLock().lock();
        try {
            while (!immutableMemTables.isEmpty()) {
                MemTable<K, V> memTable = immutableMemTables.remove(immutableMemTables.size() - 1);
                Segment<K, V> segment = Segment.fromMemTable(memTable);
                segments.add(0, segment);
            }

            // 检查是否需要Compaction
            if (segments.size() > maxSegmentCount) {
                compact();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compaction操作
     */
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            List<Segment<K, V>> toCompact = compactionStrategy.select(segments);
            if (toCompact.isEmpty()) {
                return;
            }

            Segment<K, V> merged = Segment.merge(toCompact);
            segments.removeAll(toCompact);
            segments.add(merged);
            Collections.sort(segments);

            log.info("Compacted {} segments into one, total segments: {}", toCompact.size(), segments.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取统计信息
     */
    public Stats getStats() {
        lock.readLock().lock();
        try {
            return new Stats(
                activeMemTable.size(),
                immutableMemTables.size(),
                segments.size(),
                segments.stream().mapToLong(Segment::size).sum()
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    @Getter
    public static class Stats {
        private final long activeMemTableSize;
        private final int immutableMemTableCount;
        private final int segmentCount;
        private final long totalDiskSize;

        public Stats(long activeMemTableSize, int immutableMemTableCount, int segmentCount, long totalDiskSize) {
            this.activeMemTableSize = activeMemTableSize;
            this.immutableMemTableCount = immutableMemTableCount;
            this.segmentCount = segmentCount;
            this.totalDiskSize = totalDiskSize;
        }
    }

    /**
     * 关闭LSMTree
     */
    public void close() throws IOException {
        flushToDisk();
        if (wal != null) {
            wal.close();
        }
    }
}
