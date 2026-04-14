package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;
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

    /**
     * 从数据源创建流
     */
    public <T> DataStreamImpl<T> fromSource(DataSource<T> source) {
        return new DataStreamImpl<>(jobName, source);
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
        return new DataStreamImpl<>(jobName, new CollectionSource<>(records));
    }

    /**
     * 从Socket创建流
     */
    public DataStreamImpl<String> socketTextStream(String host, int port) {
        return new DataStreamImpl<>(jobName, new SocketSource(host, port));
    }

}
