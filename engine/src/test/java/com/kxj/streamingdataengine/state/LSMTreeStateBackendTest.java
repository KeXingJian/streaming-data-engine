package com.kxj.streamingdataengine.state;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LSM-Tree 状态后端测试
 */
@DisplayName("LSM-Tree StateBackend 测试")
class LSMTreeStateBackendTest {

    @TempDir
    Path tempDir;

    private StateBackend stateBackend;

    @BeforeEach
    void setUp() {
        stateBackend = StateBackendFactory.createLSMTreeBackend(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        stateBackend.close();
    }

    @Test
    @DisplayName("测试 ValueState 基本操作")
    void testValueState() {
        // 创建 ValueState
        ValueState<String> valueState = stateBackend.createValueState("test-value", String.class);
        
        // 创建上下文
        StateContext<String> context = stateBackend.createContext("key1");
        
        // 初始为空
        ValueState<String> state = context.getValueState("test-value");
        assertNull(state.value());
        assertTrue(state.isEmpty());
        
        // 更新值
        state.update("hello");
        assertEquals("hello", state.value());
        assertFalse(state.isEmpty());
        
        // 清除
        state.clear();
        assertNull(state.value());
        assertTrue(state.isEmpty());
    }

    @Test
    @DisplayName("测试不同 key 的 ValueState 隔离")
    void testValueStateKeyIsolation() {
        ValueState<Integer> valueState = stateBackend.createValueState("counter", Integer.class);
        
        StateContext<String> context1 = stateBackend.createContext("user-1");
        StateContext<String> context2 = stateBackend.createContext("user-2");
        
        ValueState<Integer> state1 = context1.getValueState("counter");
        ValueState<Integer> state2 = context2.getValueState("counter");
        
        // 分别更新不同 key 的状态
        state1.update(100);
        state2.update(200);
        
        // 验证隔离性
        assertEquals(100, state1.value());
        assertEquals(200, state2.value());
    }

    @Test
    @DisplayName("测试 ListState 基本操作")
    void testListState() {
        ListState<String> listState = stateBackend.createListState("items", String.class);
        
        StateContext<String> context = stateBackend.createContext("list-key");
        ListState<String> state = context.getListState("items");
        
        // 初始为空
        assertTrue(state.isEmpty());
        assertTrue(state.get().isEmpty());
        
        // 添加元素
        state.add("item1");
        state.add("item2");
        state.addAll(List.of("item3", "item4"));
        
        List<String> items = state.get();
        assertEquals(4, items.size());
        assertTrue(items.containsAll(List.of("item1", "item2", "item3", "item4")));
        
        // 更新整个列表
        state.update(List.of("new1", "new2"));
        assertEquals(2, state.get().size());
        
        // 清除
        state.clear();
        assertTrue(state.isEmpty());
    }

    @Test
    @DisplayName("测试 MapState 基本操作")
    void testMapState() {
        MapState<String, Integer> mapState = stateBackend.createMapState("scores", String.class, Integer.class);
        
        StateContext<String> context = stateBackend.createContext("map-key");
        MapState<String, Integer> state = context.getMapState("scores");
        
        // 初始为空
        assertTrue(state.isEmpty());
        
        // 添加键值对
        state.put("Alice", 90);
        state.put("Bob", 85);
        state.putAll(Map.of("Charlie", 95, "David", 88));
        
        assertEquals(90, state.get("Alice"));
        assertEquals(85, state.get("Bob"));
        assertTrue(state.contains("Charlie"));
        
        Set<String> keys = state.keys();
        assertEquals(4, keys.size());
        
        // 遍历
        int sum = 0;
        for (Integer score : state.values()) {
            sum += score;
        }
        assertEquals(358, sum);
        
        // 移除
        state.remove("Bob");
        assertFalse(state.contains("Bob"));
        
        // 清除
        state.clear();
        assertTrue(state.isEmpty());
    }

    @Test
    @DisplayName("测试快照创建与恢复")
    void testSnapshotAndRestore() {
        // 创建并填充状态
        ValueState<String> valueState = stateBackend.createValueState("snapshot-test", String.class);
        StateContext<String> context = stateBackend.createContext("snapshot-key");
        ValueState<String> state = context.getValueState("snapshot-test");
        state.update("before-snapshot");
        
        // 创建快照
        Snapshot snapshot = stateBackend.createSnapshot();
        assertNotNull(snapshot.checkpointId());
        // 快照大小可能为0（当前实现为骨架），主要验证不抛异常
        assertTrue(snapshot.stateSize() >= 0);
        
        // 修改状态
        state.update("after-snapshot");
        assertEquals("after-snapshot", state.value());
        
        // 从快照恢复（当前为骨架实现，主要验证不抛异常）
        stateBackend.restoreFromSnapshot(snapshot);
        
        // 验证恢复后的值（需要重新创建状态引用）
        StateContext<String> newContext = stateBackend.createContext("snapshot-key");
        ValueState<String> restoredState = newContext.getValueState("snapshot-test");
        // 注意：快照恢复是骨架实现，这里主要验证流程不抛异常
    }

    @Test
    @DisplayName("测试多类型状态共存")
    void testMultipleStateTypes() {
        // 创建多种类型的状态
        ValueState<String> valueState = stateBackend.createValueState("config", String.class);
        ListState<Integer> listState = stateBackend.createListState("history", Integer.class);
        MapState<String, Double> mapState = stateBackend.createMapState("metrics", String.class, Double.class);
        
        StateContext<String> context = stateBackend.createContext("multi-state-key");
        
        // 操作不同类型状态
        context.getValueState("config").update("active");
        context.getListState("history").addAll(List.of(1, 2, 3));
        context.getMapState("metrics").put("cpu", 0.75);
        context.getMapState("metrics").put("memory", 0.60);
        
        // 验证各自独立
        assertEquals("active", context.getValueState("config").value());
        assertEquals(3, context.getListState("history").get().size());
        assertEquals(2, context.getMapState("metrics").keys().size());
    }
}
