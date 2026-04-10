package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;

import java.util.List;
import java.util.function.Predicate;

/**
 * 过滤算子
 */
public class FilterOperator<T> implements StreamOperator<T> {

    private final String name;
    private final Predicate<T> predicate;

    public FilterOperator(String name, Predicate<T> predicate) {
        this.name = name;
        this.predicate = predicate;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        if (predicate.test(record.value())) {
            return List.of(record);
        }
        return List.of();
    }

    @Override
    public List<StreamRecord<T>> processWatermark(Watermark watermark) {
        return List.of(); // Watermark不传递
    }
}
