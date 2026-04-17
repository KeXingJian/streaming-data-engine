package com.kxj.streamingdataengine.state.memory;

import com.kxj.streamingdataengine.state.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于内存的状态后端实现
 * 适用于测试场景或不需要持久化的场景
 */
@Slf4j
public class MemoryStateBackend implements StateBackend {
    
    private final Map<String, Object> states;
    private final Map<String, StateDescriptor<?, ?>> stateDescriptors;
    
    public MemoryStateBackend() {
        this.states = new ConcurrentHashMap<>();
        this.stateDescriptors = new ConcurrentHashMap<>();
        log.info("[kxj: MemoryStateBackend 初始化]");
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <K, V> ValueState<V> createValueState(String stateName, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor<>(stateName, valueClass, StateType.VALUE));
        return (ValueState<V>) states.computeIfAbsent(stateName, k -> new MemoryValueState<V>());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <K, V> ListState<V> createListState(String stateName, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor<>(stateName, valueClass, StateType.LIST));
        return (ListState<V>) states.computeIfAbsent(stateName, k -> new MemoryListState<V>());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <K, NK, V> MapState<NK, V> createMapState(String stateName, Class<NK> keyClass, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor<>(stateName, valueClass, StateType.MAP));
        return (MapState<NK, V>) states.computeIfAbsent(stateName, k -> new MemoryMapState<NK, V>());
    }
    
    @Override
    public <K> StateContext<K> createContext(K key) {
        return new MemoryStateContext<>(key, this);
    }
    
    @Override
    public Snapshot createSnapshot() {
        // 内存状态后端创建快照时序列化所有状态
        Map<String, byte[]> snapshotData = new HashMap<>();
        String checkpointId = "mem-chk-" + System.currentTimeMillis();
        
        states.forEach((name, state) -> {
            try {
                byte[] data = serializeState(state);
                snapshotData.put(name, data);
            } catch (Exception e) {
                log.error("[kxj: 内存状态快照序列化失败 - stateName={}]", name, e);
            }
        });
        
        Snapshot snapshot = new Snapshot(checkpointId, snapshotData);
        log.info("[kxj: 创建内存状态快照 - checkpointId={}, stateCount={}]", checkpointId, snapshotData.size());
        return snapshot;
    }
    
    @Override
    public void restoreFromSnapshot(Snapshot snapshot) {
        log.info("[kxj: 从快照恢复内存状态 - checkpointId={}]", snapshot.checkpointId());
        states.clear();
        snapshot.stateData().forEach((name, data) -> {
            try {
                Object state = deserializeState(data, stateDescriptors.get(name));
                states.put(name, state);
            } catch (Exception e) {
                log.error("[kxj: 内存状态恢复失败 - stateName={}]", name, e);
            }
        });
    }
    
    @Override
    public void close() {
        states.clear();
        stateDescriptors.clear();
        log.info("[kxj: MemoryStateBackend 已关闭]");
    }
    
    // ===== ValueState 实现 =====
    
    private static class MemoryValueState<V> implements ValueState<V> {
        private volatile V value;
        
        @Override
        public V value() {
            return value;
        }
        
        @Override
        public void update(V value) {
            this.value = value;
        }
        
        @Override
        public void clear() {
            this.value = null;
        }
    }
    
    // ===== ListState 实现 =====
    
    private static class MemoryListState<V> implements ListState<V> {
        private final List<V> list = Collections.synchronizedList(new ArrayList<>());
        
        @Override
        public List<V> get() {
            return new ArrayList<>(list);
        }
        
        @Override
        public void add(V value) {
            list.add(value);
        }
        
        @Override
        public void addAll(List<V> values) {
            list.addAll(values);
        }
        
        @Override
        public void update(List<V> values) {
            list.clear();
            if (values != null) {
                list.addAll(values);
            }
        }
        
        @Override
        public void clear() {
            list.clear();
        }
    }
    
    // ===== MapState 实现 =====
    
    private static class MemoryMapState<K, V> implements MapState<K, V> {
        private final Map<K, V> map = new ConcurrentHashMap<>();
        
        @Override
        public V get(K key) {
            return map.get(key);
        }
        
        @Override
        public void put(K key, V value) {
            map.put(key, value);
        }
        
        @Override
        public void putAll(Map<K, V> entries) {
            map.putAll(entries);
        }
        
        @Override
        public void remove(K key) {
            map.remove(key);
        }
        
        @Override
        public boolean contains(K key) {
            return map.containsKey(key);
        }
        
        @Override
        public Set<K> keys() {
            return new HashSet<>(map.keySet());
        }
        
        @Override
        public Iterable<V> values() {
            return new ArrayList<>(map.values());
        }
        
        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return new ArrayList<>(map.entrySet());
        }
        
        @Override
        public void clear() {
            map.clear();
        }
    }
    
    // ===== StateContext 实现 =====
    
    private record MemoryStateContext<K>(K key, MemoryStateBackend backend) implements StateContext<K> {
        
        @Override
        public K getCurrentKey() {
            return key;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public <V> ValueState<V> getValueState(String stateName) {
            return (ValueState<V>) backend.states.get(stateName);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public <V> ListState<V> getListState(String stateName) {
            return (ListState<V>) backend.states.get(stateName);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public <NK, V> MapState<NK, V> getMapState(String stateName) {
            return (MapState<NK, V>) backend.states.get(stateName);
        }
    }
    
    // ===== 辅助方法 =====
    
    private byte[] serializeState(Object state) throws Exception {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(state);
        oos.flush();
        return baos.toByteArray();
    }
    
    private Object deserializeState(byte[] data, StateDescriptor<?, ?> descriptor) throws Exception {
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
        return ois.readObject();
    }
    
    private record StateDescriptor<V, NK>(String name, Class<V> valueClass, StateType type) {}
    
    private enum StateType {
        VALUE, LIST, MAP
    }
}
