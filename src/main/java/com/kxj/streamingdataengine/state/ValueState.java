package com.kxj.streamingdataengine.state;

/**
 * 单值状态接口
 */
public interface ValueState<V> {
    
    /**
     * 获取当前值（无 key 场景）
     */
    V value();
    
    /**
     * 获取指定 key 的值
     */
    default V value(Object key) {
        return value();
    }
    
    /**
     * 更新值（无 key 场景）
     */
    void update(V value);
    
    /**
     * 更新指定 key 的值
     */
    default void update(V value, Object key) {
        update(value);
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
     * 是否存在值
     */
    default boolean isEmpty() {
        return value() == null;
    }
}
