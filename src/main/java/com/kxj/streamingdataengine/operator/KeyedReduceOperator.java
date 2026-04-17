package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * 流式 Keyed 归约算子
 * 逐条处理、维护 keyed 状态，processElement 返回当前 key 的最新归约结果
 */
@Slf4j
public class KeyedReduceOperator<T, K> implements StreamOperator<T> {

    private final Function<T, K> keyExtractor;
    private final BinaryOperator<T> reducer;
    private final long stateTtlMs;
    private final ConcurrentHashMap<K, T> state = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K, Long> lastAccessTime = new ConcurrentHashMap<>();

    public KeyedReduceOperator(Function<T, K> keyExtractor,
                               BinaryOperator<T> reducer) {
        this(keyExtractor, reducer, 0);
    }

    public KeyedReduceOperator(Function<T, K> keyExtractor,
                               BinaryOperator<T> reducer,
                               long stateTtlMs) {
        this.keyExtractor = keyExtractor;
        this.reducer = reducer;
        this.stateTtlMs = stateTtlMs;
    }

    @Override
    public String getName() {
        return "keyedReduce";
    }

    @Override
    public List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        T value = record.value();
        K key = keyExtractor.apply(value);
        T result = state.merge(key, value, reducer);
        lastAccessTime.put(key, record.eventTime());
        return List.of(record.withValue(result));
    }

    @Override
    public List<StreamRecord<T>> processWatermark(Watermark watermark) {
        if (stateTtlMs > 0) {
            long watermarkTime = watermark.getTimestamp();
            int cleaned = 0;
            for (K key : new java.util.ArrayList<>(lastAccessTime.keySet())) {
                Long accessTime = lastAccessTime.get(key);
                if (accessTime != null && accessTime + stateTtlMs < watermarkTime) {
                    state.remove(key);
                    lastAccessTime.remove(key);
                    cleaned++;
                }
            }
            if (cleaned > 0) {
                log.info("[kxj: KeyedReduceOperator TTL清理 - 清理超期key数量={}]", cleaned);
            }
        }
        return Collections.emptyList();
    }
}
