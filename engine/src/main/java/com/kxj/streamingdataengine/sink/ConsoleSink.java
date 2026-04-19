package com.kxj.streamingdataengine.sink;

import com.kxj.streamingdataengine.core.model.DataSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

/**
 * 控制台输出Sink
 */
@Slf4j
@RequiredArgsConstructor
public class ConsoleSink<T> implements DataSink<T> {

    private final Function<T, String> formatter;

    public ConsoleSink() {
        this(Object::toString);
    }

    @Override
    public void write(T value) {
        System.out.println(formatter.apply(value));
    }
}
