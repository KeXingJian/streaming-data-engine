package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 窗口算子抽象基类
 * 统一处理窗口分配、Trigger 调度、Watermark 驱动和状态清理
 */
@Slf4j
public abstract class AbstractWindowOperator<T, S> implements StreamOperator<T> {

    protected final Window.Assigner<T> assigner;
    protected final Trigger<T> trigger;
    protected final Function<T, ?> keyExtractor;
    protected final long allowedLateness;

    protected final ConcurrentHashMap<Window, S> nonKeyedState = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<Object, ConcurrentHashMap<Window, S>> keyedState = new ConcurrentHashMap<>();

    protected AbstractWindowOperator(Window.Assigner<T> assigner,
                                     Trigger<T> trigger,
                                     Function<T, ?> keyExtractor,
                                     long allowedLateness) {
        this.assigner = assigner;
        this.trigger = trigger != null ? trigger : assigner.getDefaultTrigger();
        this.keyExtractor = keyExtractor;
        this.allowedLateness = allowedLateness;
    }

    @Override
    public String getName() {
        return keyExtractor != null ? "keyed" + getOperatorName() : getOperatorName();
    }

    /**
     * 子类提供算子名称（如 windowAggregate）
     */
    protected abstract String getOperatorName();

    /**
     * 子类创建初始状态
     */
    protected abstract S createState(Window window, T value, Object key);

    /**
     * 子类更新状态
     */
    protected abstract S updateState(S current, T value, Window window, Object key);

    /**
     * 子类从状态提取结果
     */
    protected abstract Object extractResult(S state, Window window, Object key);

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        T value = record.value();
        long eventTime = record.eventTime();
        List<Window> windows = assigner.assignWindows(value, eventTime);
        List<StreamRecord<T>> fired = new ArrayList<>();

        for (Window window : windows) {
            Trigger.TriggerResult tr = trigger.onElement(record, eventTime, window);
            Object key = keyExtractor != null ? keyExtractor.apply(value) : null;
            S state = getOrCreateState(window, value, key);
            S updated = updateState(state, value, window, key);
            putState(window, key, updated);

            if (tr.isFire()) {
                Object result = extractResult(updated, window, key);
                fired.add((StreamRecord<T>) record.withValue(result));
            }
            if (tr.isPurge()) {
                purgeWindow(window, key);
            }
        }
        return fired.isEmpty() ? Collections.emptyList() : fired;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamRecord<T>> processWatermark(Watermark watermark) {
        List<StreamRecord<T>> fired = new ArrayList<>();
        long time = watermark.getTimestamp();
        System.out.println("[AbstractWindowOperator] processWatermark time=" + time
                + " operator=" + getName()
                + " keyed=" + (keyExtractor != null)
                + " nonKeyedStateSize=" + nonKeyedState.size()
                + " keyedStateSize=" + keyedState.size());

        if (keyExtractor != null) {
            for (Map.Entry<Object, ConcurrentHashMap<Window, S>> entry : new ArrayList<>(keyedState.entrySet())) {
                Object key = entry.getKey();
                ConcurrentHashMap<Window, S> map = entry.getValue();
                for (Map.Entry<Window, S> winEntry : new ArrayList<>(map.entrySet())) {
                    Window window = winEntry.getKey();
                    Trigger.TriggerResult tr = trigger.onEventTime(time, window);
                    if (tr.isFire()) {
                        Object result = extractResult(winEntry.getValue(), window, key);
                        fired.add((StreamRecord<T>) new StreamRecord<>(null, result, time, 0, 0));
                    }
                    if (tr.isPurge()) {
                        map.remove(window);
                    } else if (window.maxTimestamp() + allowedLateness < time) {
                        map.remove(window);
                    }
                    if (map.isEmpty()) {
                        keyedState.remove(key);
                    }
                }
            }
        } else {
            for (Map.Entry<Window, S> entry : new ArrayList<>(nonKeyedState.entrySet())) {
                Window window = entry.getKey();
                Trigger.TriggerResult tr = trigger.onEventTime(time, window);
                if (tr.isFire()) {
                    Object result = extractResult(entry.getValue(), window, null);
                    fired.add((StreamRecord<T>) new StreamRecord<>(null, result, time, 0, 0));
                }
                if (tr.isPurge()) {
                    nonKeyedState.remove(window);
                } else if (window.maxTimestamp() + allowedLateness < time) {
                    nonKeyedState.remove(window);
                }
            }
        }
        return fired;
    }

    private S getOrCreateState(Window window, T value, Object key) {
        if (key != null) {
            return keyedState.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(window, w -> createState(window, value, key));
        }
        return nonKeyedState.computeIfAbsent(window, w -> createState(window, value, null));
    }

    private void putState(Window window, Object key, S state) {
        if (key != null) {
            keyedState.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(window, state);
        } else {
            nonKeyedState.put(window, state);
        }
    }

    private void purgeWindow(Window window, Object key) {
        if (key != null) {
            ConcurrentHashMap<Window, S> map = keyedState.get(key);
            if (map != null) {
                map.remove(window);
                if (map.isEmpty()) {
                    keyedState.remove(key);
                }
            }
        } else {
            nonKeyedState.remove(window);
        }
    }
}
