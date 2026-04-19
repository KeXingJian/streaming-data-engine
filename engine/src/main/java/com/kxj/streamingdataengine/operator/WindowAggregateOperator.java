package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

/**
 * 流式窗口聚合算子
 * 按 eventTime 分配窗口，Watermark 驱动触发
 */
@Slf4j
public class WindowAggregateOperator<T, ACC, R> extends AbstractWindowOperator<T, ACC> {

    private final AggregateFunction<T, ACC, R> aggregateFunction;

    public WindowAggregateOperator(Window.Assigner<T> assigner,
                                   Trigger<T> trigger,
                                   AggregateFunction<T, ACC, R> aggregateFunction,
                                   Function<T, ?> keyExtractor,
                                   long allowedLateness) {
        super(assigner, trigger, keyExtractor, allowedLateness);
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    protected String getOperatorName() {
        return "windowAggregate";
    }

    @Override
    protected ACC createState(Window window, T value, Object key) {
        return aggregateFunction.createAccumulator();
    }

    @Override
    protected ACC updateState(ACC current, T value, Window window, Object key) {
        aggregateFunction.add(value, current);
        return current;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object extractResult(ACC state, Window window, Object key) {
        return aggregateFunction.getResult(state);
    }
}
