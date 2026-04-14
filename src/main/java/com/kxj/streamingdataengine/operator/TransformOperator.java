package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.operator.StreamOperator;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * 通用转换算子
 * 对记录值应用Function，null结果表示过滤
 */
public class TransformOperator<T> implements StreamOperator<T> {

    private final Function<Object, Object> func;

    public TransformOperator(Function<Object, Object> func) {
        this.func = func;
    }

    @Override
    public String getName() {
        return "transform";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamRecord<T>> processElement(StreamRecord<T> record) {
        Object result = func.apply(record.value());
        if (result == null) {
            return Collections.emptyList();
        }
        return List.of(record.withValue((T) result));
    }

    @Override
    public void processWatermark(Watermark watermark) {
        // 转换算子不处理Watermark
    }
}
