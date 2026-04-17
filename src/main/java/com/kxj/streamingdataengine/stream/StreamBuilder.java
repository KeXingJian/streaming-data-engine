package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.connector.kafka.KafkaSource;
import com.kxj.streamingdataengine.connector.kafka.KafkaSourceStream;
import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.watermark.WatermarkStrategy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
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
     * 从集合创建流（测试用），eventTime 为当前系统时间
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
     * 从集合创建流，使用 WatermarkStrategy 自动生成 Watermark
     */
    public <T> DataStreamImpl<T> fromCollection(List<T> data, WatermarkStrategy<T> strategy) {
        return new DataStreamImpl<>(jobName, new CollectionSource<>(data, strategy, Duration.ofMillis(50)));
    }

    /**
     * 从集合创建流，使用 WatermarkStrategy 和自定义 Watermark 间隔
     */
    public <T> DataStreamImpl<T> fromCollection(List<T> data, WatermarkStrategy<T> strategy, Duration watermarkInterval) {
        return new DataStreamImpl<>(jobName, new CollectionSource<>(data, strategy, watermarkInterval));
    }

    /**
     * 从Socket创建流
     */
    public DataStreamImpl<String> socketTextStream(String host, int port) {
        return new DataStreamImpl<>(jobName, new SocketSource(host, port));
    }

    /**
     * 从 Kafka 创建流
     * 
     * @param bootstrapServers Kafka 集群地址，如 "localhost:9092"
     * @param topic 消费主题
     * @param groupId 消费者组 ID
     * @param deserializer 值反序列化器
     * @return DataStream
     */
    public <T> KafkaSourceStream<T> fromKafka(String bootstrapServers, String topic, 
                                               String groupId, Deserializer<T> deserializer) {
        KafkaSource.KafkaSourceConfig config = KafkaSource.KafkaSourceConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topics(List.of(topic))
            .groupId(groupId)
            .build();

        KafkaSource<T> kafkaSource = new KafkaSource<>(jobName + "-source", config, deserializer);
        return new KafkaSourceStream<>(jobName, kafkaSource);
    }

    /**
     * 从 Kafka 创建流（使用 StringDeserializer）
     */
    public KafkaSourceStream<String> kafkaTextStream(String bootstrapServers, String topic, String groupId) {
        return fromKafka(bootstrapServers, topic, groupId, 
            new org.apache.kafka.common.serialization.StringDeserializer());
    }

}
