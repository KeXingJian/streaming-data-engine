package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 流式 Keyed 聚合算子
 * 逐条处理、维护 keyed 状态，processElement 返回当前 key 的最新聚合结果
 */
@Slf4j
public class KeyedAggregateOperator<T, K, ACC, R> implements StreamOperator<T> {

    private final Function<T, K> keyExtractor;
    private final AggregateFunction<T, ACC, R> aggregateFunction;
    private final long stateTtlMs;
    private final ConcurrentHashMap<K, ACC> state = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<K, Long> lastAccessTime = new ConcurrentHashMap<>();

    public KeyedAggregateOperator(Function<T, K> keyExtractor,
                                  AggregateFunction<T, ACC, R> aggregateFunction) {
        this(keyExtractor, aggregateFunction, 0);
    }

    public KeyedAggregateOperator(Function<T, K> keyExtractor,
                                  AggregateFunction<T, ACC, R> aggregateFunction,
                                  long stateTtlMs) {
        this.keyExtractor = keyExtractor;
        this.aggregateFunction = aggregateFunction;
        this.stateTtlMs = stateTtlMs;
    }

    @Override
    public String getName() {
        return "keyedAggregate";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        T value = record.value();
        K key = keyExtractor.apply(value);
        // 使用 compute 保证每个 key 的 accumulator 更新原子化
        ACC acc = state.compute(key, (k, oldAcc) -> {
            ACC a = oldAcc == null ? aggregateFunction.createAccumulator() : oldAcc;
            aggregateFunction.add(value, a);
            return a;
        });
        lastAccessTime.put(key, record.eventTime());
        R result = aggregateFunction.getResult(acc);
        // 返回包含当前 key 最新聚合结果的记录（raw type + unchecked cast 兼容类型变化）
        return List.of((StreamRecord<T>) record.withValue((T) result));
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
                log.info("[kxj: KeyedAggregateOperator TTL清理 - 清理超期key数量={}]", cleaned);
            }
        }
        // Keyed 聚合不依赖 Watermark 触发
        return Collections.emptyList();
    }
}
