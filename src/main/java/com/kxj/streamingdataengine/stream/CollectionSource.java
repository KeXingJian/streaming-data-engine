package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.core.watermark.TimestampAssigner;
import com.kxj.streamingdataengine.core.watermark.WatermarkGenerator;
import com.kxj.streamingdataengine.core.watermark.WatermarkStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

//增强集合
public class CollectionSource<T> implements DataSource<T> {
    private final List<StreamRecord<T>> records;
    private int index = 0;

    public CollectionSource(List<StreamRecord<T>> records) {
        this.records = records != null ? records : List.of();
    }

    /**
     * 从原始数据和 WatermarkStrategy 构造 Source，自动在数据间插入 Watermark
     */
    public CollectionSource(List<T> data, WatermarkStrategy<T> strategy, Duration watermarkInterval) {
        this.records = buildRecordsWithWatermarks(data, strategy, watermarkInterval);
        System.out.println("[CollectionSource] created with " + records.size() + " records");
        for (int i = 0; i < records.size(); i++) {
            StreamRecord<T> r = records.get(i);
            System.out.println("  [" + i + "] value=" + r.value() + " eventTime=" + r.eventTime());
        }
    }

    private static <T> List<StreamRecord<T>> buildRecordsWithWatermarks(List<T> data, WatermarkStrategy<T> strategy, Duration watermarkInterval) {
        if (data == null || data.isEmpty()) {
            return List.of(watermarkRecord(Watermark.endOfStream().getTimestamp()));
        }
        List<StreamRecord<T>> records = new ArrayList<>();
        TimestampAssigner<T> assigner = strategy.createTimestampAssigner();
        WatermarkGenerator<T> generator = strategy.createWatermarkGenerator();
        long intervalMs = watermarkInterval.toMillis();
        long lastWatermark = Long.MIN_VALUE;
        long seq = 0;

        for (T item : data) {
            long eventTime = assigner.extractTimestamp(item);
            records.add(new StreamRecord<>(String.valueOf(seq), item, eventTime, 0, seq++));
            generator.onEvent(item, eventTime);
            Watermark wm = generator.getCurrentWatermark();
            long wmTime = wm.getTimestamp();
            if (wmTime > lastWatermark && (lastWatermark == Long.MIN_VALUE || wmTime >= lastWatermark + intervalMs)) {
                records.add(watermarkRecord(wmTime));
                lastWatermark = wmTime;
            }
        }

        Watermark finalWm = generator.getCurrentWatermark();
        long finalTime = finalWm.getTimestamp();
        if (finalTime > lastWatermark) {
            records.add(watermarkRecord(finalTime));
        } else if (records.stream().noneMatch(r -> r.value() instanceof Watermark)) {
            // 确保至少有一个 watermark 推进窗口
            records.add(watermarkRecord(finalTime));
        }
        return records;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamRecord<T> watermarkRecord(long timestamp) {
        return (StreamRecord<T>) new StreamRecord<>(null, new Watermark(timestamp), timestamp, 0, -1);
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
