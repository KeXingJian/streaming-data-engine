package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * 流式窗口归约算子
 */
@Slf4j
public class WindowReduceOperator<T> extends AbstractWindowOperator<T, T> {

    private final BinaryOperator<T> reducer;

    public WindowReduceOperator(Window.Assigner<T> assigner,
                                Trigger<T> trigger,
                                BinaryOperator<T> reducer,
                                Function<T, ?> keyExtractor,
                                long allowedLateness) {
        super(assigner, trigger, keyExtractor, allowedLateness);
        this.reducer = reducer;
    }

    @Override
    protected String getOperatorName() {
        return "windowReduce";
    }

    @Override
    protected T createState(Window window, T value, Object key) {
        return value;
    }

    @Override
    protected T updateState(T current, T value, Window window, Object key) {
        return reducer.apply(current, value);
    }

    @Override
    protected Object extractResult(T state, Window window, Object key) {
        return state;
    }
}
