package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.WatermarkStrategy;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 数据流构建器
 * 流式API入口
 */
@RequiredArgsConstructor
public class StreamBuilder {

    private final String jobName;
    private final StreamConfig config;

    public StreamBuilder(String jobName) {
        this.jobName = jobName;
        this.config = new StreamConfig();
    }

    /**
     * 从数据源创建流
     */
    public <T> DataStreamImpl<T> fromSource(DataSource<T> source) {
        return new DataStreamImpl<>(jobName, config, source);
    }

    /**
     * 从集合创建流（测试用）
     */
    public <T> DataStreamImpl<T> fromCollection(List<T> data) {
        //为数据增加序列号和时间
        List<StreamRecord<T>> records = new ArrayList<>();
        long seq = 0;
        for (T item : data) {
            records.add(new StreamRecord<>(
                    String.valueOf(seq),
                    item,
                    System.currentTimeMillis(),
                    0,
                    seq++
            ));
        }
        return new DataStreamImpl<>(jobName, config, new CollectionSource<>(records));
    }

    /**
     * 从Socket创建流
     */
    public DataStreamImpl<String> socketTextStream(String host, int port) {
        return new DataStreamImpl<>(jobName, config, new SocketSource(host, port));
    }

    /**
     * 流配置
     */
    public StreamBuilder withParallelism(int parallelism) {
        config.setParallelism(parallelism);
        return this;
    }

    public StreamBuilder withWatermarkInterval(long intervalMs) {
        config.setWatermarkInterval(intervalMs);
        return this;
    }

    public StreamBuilder withAdaptiveWindow(boolean enable) {
        config.setEnableAdaptiveWindow(enable);
        return this;
    }

    public StreamBuilder withBackpressure(boolean enable) {
        config.setEnableBackpressure(enable);
        return this;
    }

    /**
     * 集合数据源
     */
    private static class CollectionSource<T> implements DataSource<T> {
        private final List<StreamRecord<T>> records;
        private int index = 0;

        CollectionSource(List<StreamRecord<T>> records) {
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
        public void close() {}
    }

    /**
     * Socket数据源（简化实现）
     */
    private record SocketSource(String host, int port) implements DataSource<String> {
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
}
