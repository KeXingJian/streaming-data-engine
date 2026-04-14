package com.kxj.streamingdataengine.storage.lsm;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 内存表 - 基于ConcurrentSkipListMap的有序内存结构
 * 借鉴Kafka的内存缓冲设计
 */
class MemTable<K extends Comparable<K>, V> {

    /**
     * 底层存储 - 线程安全的跳表
     */
    private final NavigableMap<K, Entry<V>> data;

    /**
     * 估计内存大小
     * -- GETTER --
     *  获取估计内存大小

     */
    @Getter
    private volatile long estimatedSize;

    public MemTable() {
        this.data = new ConcurrentSkipListMap<>();
        this.estimatedSize = 0;
    }

    /**
     * 写入数据
     */
    public void put(K key, V value, long sequenceNumber) {
        Entry<V> entry = new Entry<>(value, sequenceNumber, false);
        Entry<V> previous = data.put(key, entry);
        updateSize(key, value, previous);
    }

    /**
     * 删除数据（写入墓碑标记）
     */
    public void delete(K key, long sequenceNumber) {
        Entry<V> tombstone = new Entry<>(null, sequenceNumber, true);
        Entry<V> previous = data.put(key, tombstone);
        updateSize(key, null, previous);
    }

    /**
     * 查询数据
     */
    public Optional<Entry<V>> get(K key) {
        return Optional.ofNullable(data.get(key));
    }

    /**
     * 范围查询
     */
    public List<Map.Entry<K, Entry<V>>> range(K startKey, K endKey) {
        return new ArrayList<>(data.subMap(startKey, true, endKey, true).entrySet());
    }

    /**
     * 获取所有数据
     */
    public NavigableMap<K, Entry<V>> getAll() {
        return new TreeMap<>(data);
    }

    /**
     * 获取条目数
     */
    public int size() {
        return data.size();
    }

    /**
     * 判断是否为空
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * 更新内存大小估计
     */
    private void updateSize(K key, V value, Entry<V> previous) {
        // 简单估计：键值对数量 * 平均大小
        long entrySize = 64; // 基础开销
        if (key != null) {
            entrySize += estimateObjectSize(key);
        }
        if (value != null) {
            entrySize += estimateObjectSize(value);
        }

        if (previous == null) {
            estimatedSize += entrySize;
        }
    }

    /**
     * 估计对象大小（简化实现）
     */
    private long estimateObjectSize(Object obj) {
        return switch (obj) {
            case null -> 0;
            case String s -> s.length() * 2L + 40;
            case byte[] bytes -> bytes.length + 16;
            default -> 64;
        };
    }
}

/**
 * 条目封装 - 包含值、序列号和删除标记
 */
record Entry<V>(V value, long sequenceNumber, boolean deleted) {

}
