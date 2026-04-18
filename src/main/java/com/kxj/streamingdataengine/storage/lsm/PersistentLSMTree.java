package com.kxj.streamingdataengine.storage.lsm;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 持久化 LSM-Tree (Log-Structured Merge-Tree)
 * 
 * 相比基础版 LSMTree 的改进：
 * 1. 使用指定持久化路径（非临时目录）
 * 2. 启动时自动恢复 Segment
 * 3. 支持关闭时刷盘保证数据完整性
 * 
 * 借鉴 Kafka 的日志结构 + RocksDB 的持久化设计
 */
@Slf4j
public class PersistentLSMTree<K extends Comparable<K>, V> {

    private final Path basePath;          // 持久化目录
    private final Path dataPath;          // 数据目录
    private final Path walPath;           // WAL 目录
    
    private final int memTableSize;       // 内存表大小阈值
    private final int maxSegmentCount;    // 最大 Segment 数量

    private volatile MemTable<K, V> activeMemTable;
    private final List<MemTable<K, V>> immutableMemTables;
    private final List<Segment<K, V>> segments;

    private final AtomicLong sequenceNumber;
    private final ReadWriteLock lock;
    private final CompactionStrategy compactionStrategy;
    private final WriteAheadLog wal;

    private volatile boolean closed = false;

    /**
     * 创建持久化 LSM-Tree
     * 
     * @param basePath 持久化目录路径
     * @param memTableSize MemTable 大小阈值（字节）
     * @param maxSegmentCount 最大 Segment 数量
     */
    public PersistentLSMTree(String basePath, int memTableSize, int maxSegmentCount) throws IOException {
        this.basePath = Paths.get(basePath);
        this.dataPath = this.basePath.resolve("data");
        this.walPath = this.basePath.resolve("wal");
        
        this.memTableSize = memTableSize;
        this.maxSegmentCount = maxSegmentCount;
        
        // 创建目录
        Files.createDirectories(dataPath);
        Files.createDirectories(walPath);
        
        // 初始化 WAL
        this.wal = new WriteAheadLog(walPath);
        
        this.compactionStrategy = new SizeTieredCompaction();
        this.activeMemTable = new MemTable<>();
        this.immutableMemTables = Collections.synchronizedList(new ArrayList<>());
        this.segments = Collections.synchronizedList(new ArrayList<>());
        this.sequenceNumber = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        
        // 启动时恢复数据
        recover();
        
        log.info("[kxj: PersistentLSMTree 初始化完成] path={}, segments={}", 
                basePath, segments.size());
    }

    /**
     * 默认构造函数（使用系统临时目录 - 仅用于测试）
     */
    public PersistentLSMTree() throws IOException {
        this(System.getProperty("java.io.tmpdir") + "/lsm-tree-" + System.currentTimeMillis(),
             64 * 1024 * 1024, 10);
    }

    /**
     * 写入数据
     */
    public void put(K key, V value) {
        checkNotClosed();
        lock.writeLock().lock();
        try {
            // [kxj: LSM-Tree写入流程 - 先写WAL保证持久化，再写MemTable]
            if (wal != null) {
                wal.append(key, value);
            }

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
        checkNotClosed();
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
        checkNotClosed();
        lock.readLock().lock();
        try {
            // [kxj: LSM-Tree查询顺序 - 活跃MemTable → 不可变MemTable → 磁盘Segment]
            // 1. 查活跃MemTable
            Optional<Entry<V>> entry = activeMemTable.get(key);
            if (entry.isPresent()) {
                return entry.get().deleted() ? Optional.empty() : Optional.of(entry.get().value());
            }

            // 2. 查不可变MemTable（从新到旧）
            for (MemTable<K, V> memTable : immutableMemTables) {
                entry = memTable.get(key);
                if (entry.isPresent()) {
                    return entry.get().deleted() ? Optional.empty() : Optional.of(entry.get().value());
                }
            }

            // 3. 查磁盘Segment（从新到旧）
            for (Segment<K, V> segment : segments) {
                entry = segment.get(key);
                if (entry.isPresent()) {
                    return entry.get().deleted() ? Optional.empty() : Optional.of(entry.get().value());
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
        checkNotClosed();
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
                if (!e.getValue().deleted()) {
                    filtered.add(Map.entry(e.getKey(), e.getValue().value()));
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
        checkNotClosed();
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
     * 刷盘 MemTable
     */
    private void flushMemTable() {
        immutableMemTables.addFirst(activeMemTable);
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
     * 写入磁盘（持久化到指定目录）
     */
    private void flushToDisk() throws IOException {
        lock.writeLock().lock();
        try {
            while (!immutableMemTables.isEmpty()) {
                MemTable<K, V> memTable = immutableMemTables.removeLast();
                
                // 生成 Segment 文件名（基于序列号）
                String segmentId = String.format("%010d", sequenceNumber.get());
                Segment<K, V> segment = Segment.fromMemTable(memTable, dataPath, segmentId);
                
                segments.addFirst(segment);
                log.info("[kxj: Segment 持久化完成] segmentId={}, entries={}", 
                        segmentId, memTable.size());
            }

            // 检查是否需要 Compaction
            if (segments.size() > maxSegmentCount) {
                compact();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compaction 操作
     */
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            List<Segment<K, V>> toCompact = compactionStrategy.select(segments);
            if (toCompact.isEmpty()) {
                return;
            }

            String mergedId = String.format("merged-%010d", sequenceNumber.get());
            Segment<K, V> merged = Segment.merge(toCompact, dataPath, mergedId);
            
            segments.removeAll(toCompact);
            segments.add(merged);
            Collections.sort(segments);

            log.info("[kxj: Compaction 完成] 合并 {} 个 Segments 为 {}，总 Segments: {}", 
                    toCompact.size(), mergedId, segments.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 启动时恢复数据
     */
    private void recover() throws IOException {
        log.info("[kxj: 开始恢复 LSM-Tree 数据]");
        
        // 1. 从 WAL 恢复未刷盘的数据
        if (wal != null) {
            AtomicLong recoveredCount = new AtomicLong(0);
            wal.recover(new WriteAheadLog.WALHandler() {
                @Override
                public void onPut(Object key, Object value) {
                    @SuppressWarnings("unchecked")
                    K k = (K) key;
                    @SuppressWarnings("unchecked")
                    V v = (V) value;
                    activeMemTable.put(k, v, sequenceNumber.incrementAndGet());
                    recoveredCount.incrementAndGet();
                }

                @Override
                public void onDelete(Object key) {
                    @SuppressWarnings("unchecked")
                    K k = (K) key;
                    activeMemTable.delete(k, sequenceNumber.incrementAndGet());
                }
            });
            
            if (recoveredCount.get() > 0) {
                log.info("[kxj: 从 WAL 恢复 {} 条记录]", recoveredCount.get());
            }
        }
        
        // 2. 加载已有的 Segment 文件
        if (Files.exists(dataPath)) {
            List<Path> segmentFiles = Files.list(dataPath)
                .filter(p -> p.toString().endsWith(".data"))
                .sorted()
                .toList();
            
            for (Path segmentFile : segmentFiles) {
                try {
                    Segment<K, V> segment = Segment.load(segmentFile);
                    segments.add(segment);
                    log.debug("[kxj: 加载 Segment] {}", segmentFile.getFileName());
                } catch (IOException e) {
                    log.error("[kxj: 加载 Segment 失败] {}", segmentFile, e);
                }
            }
            
            Collections.sort(segments);
            log.info("[kxj: 从磁盘加载 {} 个 Segments]", segments.size());
        }
        
        // 3. 清空 WAL（已恢复的数据可以清理）
        if (wal != null) {
            wal.clear();
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

    /**
     * 关闭 LSM-Tree（刷盘所有数据）
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        closed = true;
        log.info("[kxj: PersistentLSMTree 关闭中，刷盘数据...]");
        
        // 刷盘所有 MemTable
        flushToDisk();
        
        // 关闭 WAL
        if (wal != null) {
            wal.close();
        }
        
        log.info("[kxj: PersistentLSMTree 已关闭]");
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("PersistentLSMTree is closed");
        }
    }

    public record Stats(long activeMemTableSize, int immutableMemTableCount, int segmentCount, long totalDiskSize) {
    }
}
