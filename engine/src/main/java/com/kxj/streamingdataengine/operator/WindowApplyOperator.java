package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * 流式窗口 Apply 算子
 * 收集窗口内所有元素，触发时调用 windowFunction.apply(list)
 */
@Slf4j
public class WindowApplyOperator<T, R> extends AbstractWindowOperator<T, List<T>> {

    private final Function<Iterable<T>, R> windowFunction;

    public WindowApplyOperator(Window.Assigner<T> assigner,
                               Trigger<T> trigger,
                               Function<Iterable<T>, R> windowFunction,
                               Function<T, ?> keyExtractor,
                               long allowedLateness) {
        super(assigner, trigger, keyExtractor, allowedLateness);
        this.windowFunction = windowFunction;
    }

    @Override
    protected String getOperatorName() {
        return "windowApply";
    }

    @Override
    protected List<T> createState(Window window, T value, Object key) {
        List<T> list = new ArrayList<>();
        list.add(value);
        return Collections.synchronizedList(list);
    }

    @Override
    protected List<T> updateState(List<T> current, T value, Window window, Object key) {
        current.add(value);
        return current;
    }

    @Override
    protected Object extractResult(List<T> state, Window window, Object key) {
        return windowFunction.apply(state);
    }
}
