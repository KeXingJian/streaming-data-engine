package com.kxj.streamingdataengine.operator;

import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 流式窗口 Fold 算子
 */
@Slf4j
public class WindowFoldOperator<T, R> extends AbstractWindowOperator<T, R> {

    private final R initialValue;
    private final BiFunction<R, T, R> folder;

    public WindowFoldOperator(Window.Assigner<T> assigner,
                              Trigger<T> trigger,
                              R initialValue,
                              BiFunction<R, T, R> folder,
                              Function<T, ?> keyExtractor,
                              long allowedLateness) {
        super(assigner, trigger, keyExtractor, allowedLateness);
        this.initialValue = initialValue;
        this.folder = folder;
    }

    @Override
    protected String getOperatorName() {
        return "windowFold";
    }

    @Override
    protected R createState(Window window, T value, Object key) {
        return folder.apply(initialValue, value);
    }

    @Override
    protected R updateState(R current, T value, Window window, Object key) {
        return folder.apply(current, value);
    }

    @Override
    protected Object extractResult(R state, Window window, Object key) {
        return state;
    }
}
