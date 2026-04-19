package com.kxj.streamingdataengine.sink;

import com.kxj.streamingdataengine.core.model.DataSink;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 集合收集Sink
 */
public class CollectSink<T> implements DataSink<T> {

    @Getter
    private final List<T> collected = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void write(T value) {
        collected.add(value);
    }

    public List<T> getAndClear() {
        List<T> result = new ArrayList<>(collected);
        collected.clear();
        return result;
    }
}
