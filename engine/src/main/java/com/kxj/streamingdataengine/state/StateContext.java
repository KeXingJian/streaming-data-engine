package com.kxj.streamingdataengine.state;

/**
 * 状态访问上下文
 * 用于在 Keyed 环境下访问状态
 */
public interface StateContext<K> {
    
    /**
     * 获取当前 key
     */
    K getCurrentKey();
    
    /**
     * 访问 ValueState
     */
    <V> ValueState<V> getValueState(String stateName);
    
    /**
     * 访问 ListState
     */
    <V> ListState<V> getListState(String stateName);
    
    /**
     * 访问 MapState
     */
    <NK, V> MapState<NK, V> getMapState(String stateName);
}
