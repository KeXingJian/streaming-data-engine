package com.kxj.streamingdataengine.state;

/**
 * 状态后端接口
 * 提供键值状态的存储和访问能力
 */
public interface StateBackend {
    
    /**
     * 创建 ValueState
     */
    <K, V> ValueState<V> createValueState(String stateName, Class<V> valueClass);
    
    /**
     * 创建 ListState
     */
    <K, V> ListState<V> createListState(String stateName, Class<V> valueClass);
    
    /**
     * 创建 MapState
     */
    <K, NK, V> MapState<NK, V> createMapState(String stateName, Class<NK> keyClass, Class<V> valueClass);
    
    /**
     * 为指定 key 创建状态访问上下文
     */
    <K> StateContext<K> createContext(K key);
    
    /**
     * 创建快照
     */
    Snapshot createSnapshot();
    
    /**
     * 从快照恢复
     */
    void restoreFromSnapshot(Snapshot snapshot);
    
    /**
     * 关闭状态后端
     */
    void close();
}
