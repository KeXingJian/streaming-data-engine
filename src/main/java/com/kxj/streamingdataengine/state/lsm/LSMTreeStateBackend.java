package com.kxj.streamingdataengine.state.lsm;

import com.kxj.streamingdataengine.state.*;
import com.kxj.streamingdataengine.storage.lsm.LSMTree;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 LSM-Tree 的状态后端实现
 * 支持 ValueState、ListState、MapState 的持久化存储
 */
@Slf4j
public class LSMTreeStateBackend implements StateBackend {
    
    private final Path basePath;
    private final LSMTree<String, byte[]> lsmTree;
    private final Map<String, StateDescriptor> stateDescriptors;
    private final AtomicLong sequenceNumber;
    
    public LSMTreeStateBackend(Path basePath) {
        this.basePath = basePath;
        this.lsmTree = new LSMTree<>();
        this.stateDescriptors = new ConcurrentHashMap<>();
        this.sequenceNumber = new AtomicLong(0);
        log.info("[kxj: LSMTreeStateBackend 初始化 - 路径={}]", basePath);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <K, V> ValueState<V> createValueState(String stateName, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor(stateName, valueClass, StateType.VALUE));
        return new LSMValueState(stateName);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <K, V> ListState<V> createListState(String stateName, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor(stateName, valueClass, StateType.LIST));
        return new LSMListState(stateName);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <K, NK, V> MapState<NK, V> createMapState(String stateName, Class<NK> keyClass, Class<V> valueClass) {
        stateDescriptors.put(stateName, new StateDescriptor(stateName, valueClass, StateType.MAP));
        return new LSMMapState(stateName);
    }
    
    @Override
    public <K> StateContext<K> createContext(K key) {
        return new LSMStateContext<>(key);
    }
    
    @Override
    public Snapshot createSnapshot() {
        String checkpointId = "chk-" + sequenceNumber.incrementAndGet();
        Map<String, byte[]> snapshotData = new HashMap<>();
        
        log.info("[kxj: 创建状态快照 - checkpointId={}, stateCount={}]", checkpointId, stateDescriptors.size());
        return new Snapshot(checkpointId, snapshotData);
    }
    
    @Override
    public void restoreFromSnapshot(Snapshot snapshot) {
        log.info("[kxj: 从快照恢复状态 - checkpointId={}]", snapshot.checkpointId());
    }
    
    @Override
    public void close() {
        try {
            lsmTree.close();
            log.info("[kxj: LSMTreeStateBackend 已关闭]");
        } catch (IOException e) {
            log.error("[kxj: 关闭状态后端失败]", e);
        }
    }
    
    // ===== 内部实现 =====
    
    private String buildKey(String stateName, Object key) {
        return stateName + ":" + (key != null ? key.toString() : "_null_");
    }
    
    // ===== ValueState 实现 =====
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private class LSMValueState implements ValueState {
        private final String stateName;
        
        LSMValueState(String stateName) {
            this.stateName = stateName;
        }
        
        @Override
        public Object value() {
            return value(null);
        }
        
        @Override
        public Object value(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            Optional<byte[]> data = lsmTree.get(fullKey);
            return data.map(this::deserialize).orElse(null);
        }
        
        @Override
        public void update(Object value) {
            update(value, null);
        }
        
        @Override
        public void update(Object value, Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            if (value == null) {
                lsmTree.delete(fullKey);
            } else {
                lsmTree.put(fullKey, serialize(value));
            }
        }
        
        @Override
        public void clear() {
            clear(null);
        }
        
        @Override
        public void clear(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            lsmTree.delete(fullKey);
        }
        
        private byte[] serialize(Object value) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(value);
                oos.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize value", e);
            }
        }
        
        private Object deserialize(byte[] data) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais);
                return ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize value", e);
            }
        }
    }
    
    // ===== ListState 实现 =====
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private class LSMListState implements ListState {
        private final String stateName;
        
        LSMListState(String stateName) {
            this.stateName = stateName;
        }
        
        @Override
        public List get() {
            return get(null);
        }
        
        @Override
        public List get(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            Optional<byte[]> data = lsmTree.get(fullKey);
            return data.map(this::deserializeList).orElse(new ArrayList<>());
        }
        
        @Override
        public void add(Object value) {
            add(value, null);
        }
        
        @Override
        public void add(Object value, Object currentKey) {
            List list = get(currentKey);
            list.add(value);
            update(list, currentKey);
        }
        
        @Override
        public void addAll(List values) {
            addAll(values, null);
        }
        
        @Override
        public void addAll(List values, Object currentKey) {
            List list = get(currentKey);
            list.addAll(values);
            update(list, currentKey);
        }
        
        @Override
        public void update(List values) {
            update(values, null);
        }
        
        @Override
        public void update(List values, Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            if (values == null || values.isEmpty()) {
                lsmTree.delete(fullKey);
            } else {
                lsmTree.put(fullKey, serializeList(values));
            }
        }
        
        @Override
        public void clear() {
            clear(null);
        }
        
        @Override
        public void clear(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            lsmTree.delete(fullKey);
        }
        
        private byte[] serializeList(List list) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(list);
                oos.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize list", e);
            }
        }
        
        private List deserializeList(byte[] data) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais);
                return (List) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize list", e);
            }
        }
    }
    
    // ===== MapState 实现 =====
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private class LSMMapState implements MapState {
        private final String stateName;
        
        LSMMapState(String stateName) {
            this.stateName = stateName;
        }
        
        @Override
        public Object get(Object key) {
            return get(key, null);
        }
        
        @Override
        public Object get(Object key, Object currentKey) {
            Map map = getMap(currentKey);
            return map.get(key);
        }
        
        @Override
        public void put(Object key, Object value) {
            put(key, value, null);
        }
        
        @Override
        public void put(Object key, Object value, Object currentKey) {
            Map map = getMap(currentKey);
            map.put(key, value);
            putMap(map, currentKey);
        }
        
        @Override
        public void putAll(Map entries) {
            putAll(entries, null);
        }
        
        @Override
        public void putAll(Map entries, Object currentKey) {
            Map map = getMap(currentKey);
            map.putAll(entries);
            putMap(map, currentKey);
        }
        
        @Override
        public void remove(Object key) {
            remove(key, null);
        }
        
        @Override
        public void remove(Object key, Object currentKey) {
            Map map = getMap(currentKey);
            map.remove(key);
            putMap(map, currentKey);
        }
        
        @Override
        public boolean contains(Object key) {
            return contains(key, null);
        }
        
        @Override
        public boolean contains(Object key, Object currentKey) {
            return getMap(currentKey).containsKey(key);
        }
        
        @Override
        public Set keys() {
            return keys(null);
        }
        
        @Override
        public Set keys(Object currentKey) {
            return getMap(currentKey).keySet();
        }
        
        @Override
        public Iterable values() {
            return values(null);
        }
        
        @Override
        public Iterable values(Object currentKey) {
            return getMap(currentKey).values();
        }
        
        @Override
        public Iterable entries() {
            return entries(null);
        }
        
        @Override
        public Iterable entries(Object currentKey) {
            return getMap(currentKey).entrySet();
        }
        
        @Override
        public void clear() {
            clear(null);
        }
        
        @Override
        public void clear(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            lsmTree.delete(fullKey);
        }
        
        private Map getMap(Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            Optional<byte[]> data = lsmTree.get(fullKey);
            return data.map(this::deserializeMap).orElse(new HashMap<>());
        }
        
        private void putMap(Map map, Object currentKey) {
            String fullKey = buildKey(stateName, currentKey);
            if (map.isEmpty()) {
                lsmTree.delete(fullKey);
            } else {
                lsmTree.put(fullKey, serializeMap(map));
            }
        }
        
        private byte[] serializeMap(Map map) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(map);
                oos.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize map", e);
            }
        }
        
        private Map deserializeMap(byte[] data) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais);
                return (Map) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize map", e);
            }
        }
    }
    
    // ===== StateContext 实现 =====
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private class LSMStateContext<K> implements StateContext<K> {
        private final K key;
        
        LSMStateContext(K key) {
            this.key = key;
        }
        
        @Override
        public K getCurrentKey() {
            return key;
        }
        
        @Override
        public ValueState getValueState(String stateName) {
            StateDescriptor descriptor = stateDescriptors.get(stateName);
            if (descriptor == null) {
                throw new IllegalArgumentException("State not found: " + stateName);
            }
            return new LSMValueState(stateName) {
                @Override
                public Object value() {
                    return value(key);
                }
                @Override
                public void update(Object value) {
                    update(value, LSMStateContext.this.key);
                }
                @Override
                public void clear() {
                    clear(LSMStateContext.this.key);
                }
            };
        }
        
        @Override
        public ListState getListState(String stateName) {
            StateDescriptor descriptor = stateDescriptors.get(stateName);
            if (descriptor == null) {
                throw new IllegalArgumentException("State not found: " + stateName);
            }
            return new LSMListState(stateName) {
                @Override
                public List get() {
                    return get(key);
                }
                @Override
                public void add(Object value) {
                    add(value, LSMStateContext.this.key);
                }
                @Override
                public void addAll(List values) {
                    addAll(values, LSMStateContext.this.key);
                }
                @Override
                public void update(List values) {
                    update(values, LSMStateContext.this.key);
                }
                @Override
                public void clear() {
                    clear(LSMStateContext.this.key);
                }
            };
        }
        
        @Override
        public MapState getMapState(String stateName) {
            StateDescriptor descriptor = stateDescriptors.get(stateName);
            if (descriptor == null) {
                throw new IllegalArgumentException("State not found: " + stateName);
            }
            return new LSMMapState(stateName) {
                @Override
                public Object get(Object key) {
                    return get(key, LSMStateContext.this.key);
                }
                @Override
                public void put(Object key, Object value) {
                    put(key, value, LSMStateContext.this.key);
                }
                @Override
                public void putAll(Map entries) {
                    putAll(entries, LSMStateContext.this.key);
                }
                @Override
                public void remove(Object key) {
                    remove(key, LSMStateContext.this.key);
                }
                @Override
                public boolean contains(Object key) {
                    return contains(key, LSMStateContext.this.key);
                }
                @Override
                public Set keys() {
                    return keys(LSMStateContext.this.key);
                }
                @Override
                public Iterable values() {
                    return values(LSMStateContext.this.key);
                }
                @Override
                public Iterable entries() {
                    return entries(LSMStateContext.this.key);
                }
                @Override
                public void clear() {
                    clear(LSMStateContext.this.key);
                }
            };
        }
    }
    
    // ===== 辅助类 =====
    
    private record StateDescriptor(String name, Class<?> valueClass, StateType type) {}
    
    private enum StateType {
        VALUE, LIST, MAP
    }
}
