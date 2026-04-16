package com.kxj.streamingdataengine.storage.lsm;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 内存表 - 基于ConcurrentSkipListMap的有序内存结构
 * 借鉴Kafka的内存缓冲设计
 */
class MemTable<K extends Comparable<K>, V> {

    private final NavigableMap<K, Entry<V>> data = new ConcurrentSkipListMap<>(); // 底层存储 - 线程安全的跳表

    /**
     * 写入数据
     */
    public void put(K key, V value, long sequenceNumber) {
        Entry<V> entry = new Entry<>(value, sequenceNumber, false);
        data.put(key, entry);
    }

    /**
     * 删除数据（写入墓碑标记）
     */
    public void delete(K key, long sequenceNumber) {
        Entry<V> tombstone = new Entry<>(null, sequenceNumber, true);
        data.put(key, tombstone);
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
}

/**
 * 条目封装 - 包含值、序列号和删除标记
 */
record Entry<V>(V value, long sequenceNumber, boolean deleted) {

}
