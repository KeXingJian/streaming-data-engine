package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;

import java.util.List;

//增强集合
public class CollectionSource<T> implements DataSource<T> {
    private final List<StreamRecord<T>> records;
    private int index = 0;

    public CollectionSource(List<StreamRecord<T>> records) {
        this.records = records != null ? records : List.of();
    }

    @Override
    public StreamRecord<T> nextRecord() {
        if (!hasMore()) {
            return null;
        }
        return records.get(index++);
    }

    @Override
    public boolean hasMore() {
        return index < records.size();
    }

    @Override
    public void close() {

    }
}
