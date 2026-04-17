package com.kxj.streamingdataengine.state;

import java.util.Map;
import java.util.Set;

/**
 * Map 状态接口
 */
public interface MapState<K, V> {
    
    // ===== 带 key 参数的方法（用于 Keyed 场景）=====
    
    /**
     * 获取指定 key 的值（带 key 参数）
     */
    default V get(K key, Object currentKey) {
        return get(key);
    }
    
    /**
     * 添加键值对（带 key 参数）
     */
    default void put(K key, V value, Object currentKey) {
        put(key, value);
    }
    
    /**
     * 批量添加（带 key 参数）
     */
    default void putAll(Map<K, V> entries, Object currentKey) {
        putAll(entries);
    }
    
    /**
     * 移除指定 key（带 key 参数）
     */
    default void remove(K key, Object currentKey) {
        remove(key);
    }
    
    /**
     * 判断是否包含 key（带 key 参数）
     */
    default boolean contains(K key, Object currentKey) {
        return contains(key);
    }
    
    /**
     * 获取所有键（带 key 参数）
     */
    default Set<K> keys(Object currentKey) {
        return keys();
    }
    
    /**
     * 获取所有值（带 key 参数）
     */
    default Iterable<V> values(Object currentKey) {
        return values();
    }
    
    /**
     * 获取所有键值对（带 key 参数）
     */
    default Iterable<Map.Entry<K, V>> entries(Object currentKey) {
        return entries();
    }
    
    /**
     * 清除状态（带 key 参数）
     */
    default void clear(Object currentKey) {
        clear();
    }
    
    // ===== 无 key 参数的方法（用于非 Keyed 场景）=====
    
    /**
     * 获取指定 key 的值
     */
    V get(K key);
    
    /**
     * 添加键值对
     */
    void put(K key, V value);
    
    /**
     * 批量添加
     */
    void putAll(Map<K, V> entries);
    
    /**
     * 移除指定 key
     */
    void remove(K key);
    
    /**
     * 判断是否包含 key
     */
    boolean contains(K key);
    
    /**
     * 获取所有键
     */
    Set<K> keys();
    
    /**
     * 获取所有值
     */
    Iterable<V> values();
    
    /**
     * 获取所有键值对
     */
    Iterable<Map.Entry<K, V>> entries();
    
    /**
     * 清除状态
     */
    void clear();
    
    /**
     * 是否为空
     */
    default boolean isEmpty() {
        return keys() == null || keys().isEmpty();
    }
}
