package com.kxj.streamingdataengine.state;

import java.util.List;

/**
 * 列表状态接口
 */
public interface ListState<V> {
    
    /**
     * 获取所有值（无 key 场景）
     */
    List<V> get();
    
    /**
     * 获取指定 key 的所有值
     */
    default List<V> get(Object key) {
        return get();
    }
    
    /**
     * 添加单个元素（无 key 场景）
     */
    void add(V value);
    
    /**
     * 添加单个元素到指定 key
     */
    default void add(V value, Object key) {
        add(value);
    }
    
    /**
     * 添加多个元素（无 key 场景）
     */
    void addAll(List<V> values);
    
    /**
     * 添加多个元素到指定 key
     */
    default void addAll(List<V> values, Object key) {
        addAll(values);
    }
    
    /**
     * 更新整个列表（无 key 场景）
     */
    void update(List<V> values);
    
    /**
     * 更新指定 key 的列表
     */
    default void update(List<V> values, Object key) {
        update(values);
    }
    
    /**
     * 清除状态（无 key 场景）
     */
    void clear();
    
    /**
     * 清除指定 key 的状态
     */
    default void clear(Object key) {
        clear();
    }
    
    /**
     * 是否为空
     */
    default boolean isEmpty() {
        return get() == null || get().isEmpty();
    }
}
