package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;

public record SocketSource(String host, int port) implements DataSource<String> {
    @Override
    public StreamRecord<String> nextRecord() {
        return null; // 简化实现
    }

    @Override
    public boolean hasMore() {
        return true;
    }

    @Override
    public void close() {}
}
