package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;

import java.util.List;
import java.util.function.Function;

/**
 * Map转换算子
 */
public class MapOperator<T, R> implements StreamOperator<R> {

    private final String name;
    private final Function<T, R> mapper;

    public MapOperator(String name, Function<T, R> mapper) {
        this.name = name;
        this.mapper = mapper;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamRecord<R>> processElement(StreamRecord<R> record) {
        // 实际类型是T，需要转换
        T value = (T) record.getValue();
        R result = mapper.apply(value);
        return List.of(record.withValue(result));
    }

    @Override
    public List<StreamRecord<R>> processWatermark(Watermark watermark) {
        return List.of();
    }
}
