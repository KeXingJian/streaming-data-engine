package com.kxj.streamingdataengine.window;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.storage.lsm.LSMTree;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 窗口状态管理
 * 管理窗口内的数据和聚合状态
 */
@Slf4j
public class WindowState<K, T, ACC, R> {

    private final LSMTree<WindowKey<K>, WindowAccumulator<T, ACC, R>> stateStore; // 窗口存储：window -> key -> accumulator

    private final Set<Window> activeWindows; // 活跃窗口集合

    private final Map<Window, WindowMetadata> metadata; // 窗口元数据：记录数、最后更新时间等

    public WindowState() {
        this.stateStore = new LSMTree<>();
        this.activeWindows = ConcurrentHashMap.newKeySet();
        this.metadata = new ConcurrentHashMap<>();
    }

    /**
     * 添加元素到窗口
     */
    public void addElement(Window window, K key, StreamRecord<T> record) {
        activeWindows.add(window);

        WindowKey<K> windowKey = new WindowKey<>(window, key);
        WindowAccumulator<T, ACC, R> acc = stateStore.get(windowKey)
                .orElse(new WindowAccumulator<>());

        acc.add(record);
        stateStore.put(windowKey, acc);

        // 更新元数据
        metadata.compute(window, (w, m) -> {
            if (m == null) {
                return new WindowMetadata(1, System.currentTimeMillis());
            }
            m.recordCount++;
            m.lastUpdateTime = System.currentTimeMillis();
            return m;
        });
    }

    /**
     * 获取窗口内的所有聚合结果
     */
    public Map<K, R> getWindowResult(Window window) {
        Map<K, R> result = new HashMap<>();

        // 范围查询：找到所有该窗口的key
        WindowKey<K> startKey = new WindowKey<>(window, null);
        WindowKey<K> endKey = new WindowKey<>(window, null);

        // 这里简化实现，实际应该用更高效的范围查询
        // 遍历所有key找到属于该窗口的
        // 实际生产环境需要更高效的索引

        return result;
    }

    /**
     * 清理窗口
     */
    public void purgeWindow(Window window) {
        activeWindows.remove(window);
        metadata.remove(window);
        // 实际的stateStore清理应该异步进行
    }

    /**
     * 获取活跃窗口
     */
    public Set<Window> getActiveWindows() {
        return new HashSet<>(activeWindows);
    }

    /**
     * 获取窗口元数据
     */
    public WindowMetadata getMetadata(Window window) {
        return metadata.get(window);
    }

    /**
     * 窗口+键的组合键
     */
    private record WindowKey<K>(Window window, K key) implements Comparable<WindowKey<K>> {
        @Override
        public int compareTo(WindowKey<K> other) {
            int cmp = window.compareTo(other.window);
            if (cmp != 0) return cmp;
            if (key == null) return other.key == null ? 0 : -1;
            if (other.key == null) return 1;
            return key.toString().compareTo(other.key.toString());
        }
    }

    /**
     * 窗口累加器
     */
    private static class WindowAccumulator<T, ACC, R> {
        private final List<StreamRecord<T>> records = new ArrayList<>();
        private ACC accumulator;

        void add(StreamRecord<T> record) {
            records.add(record);
        }

        List<StreamRecord<T>> getRecords() {
            return records;
        }
    }

    /**
     * 窗口元数据
     */
    @AllArgsConstructor
    public static class WindowMetadata {
        public long recordCount;
        public long lastUpdateTime;
    }
}
