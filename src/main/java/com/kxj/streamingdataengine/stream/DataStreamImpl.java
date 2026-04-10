package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.*;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * DataStream实现
 */
@Slf4j
public class DataStreamImpl<T> implements DataStream<T> {

    private final String jobName;
    private final StreamConfig config;
    private final DataSource<T> source;

    // 转换链：存储从source到当前类型的所有转换函数
    private final List<Function<?, ?>> transformations;

    private final List<DataSink<T>> sinks = new CopyOnWriteArrayList<>();
    private WatermarkStrategy<T> watermarkStrategy;

    private ExecutionEngine engine;

    // 构造源流
    public DataStreamImpl(String jobName, StreamConfig config, DataSource<T> source) {
        this.jobName = jobName;
        this.config = config;
        this.source = source;
        this.transformations = new ArrayList<>();
    }

    // 构造转换流
    private DataStreamImpl(String jobName, StreamConfig config, DataSource<?> source,
                           List<Function<?, ?>> transformations) {
        this.jobName = jobName;
        this.config = config;
        this.source = (DataSource<T>) source;
        this.transformations = new ArrayList<>(transformations);
    }

    @Override
    public String getId() {
        return jobName;
    }

    @Override
    public int getParallelism() {
        return config.getParallelism();
    }

    @Override
    public <R> DataStream<R> map(Function<T, R> mapper) {
        List<Function<?, ?>> newTransformations = new ArrayList<>(transformations);
        newTransformations.add(mapper);
        return new DataStreamImpl<>(jobName + "_map", config, source, newTransformations);
    }

    @Override
    public DataStream<T> filter(Predicate<T> predicate) {
        List<Function<?, ?>> newTransformations = new ArrayList<>(transformations);
        newTransformations.add((Function<T, T>) v -> predicate.test(v) ? v : null);
        return new DataStreamImpl<>(jobName + "_filter", config, source, newTransformations);
    }

    @Override
    public <K> KeyedStream<T, K> keyBy(Function<T, K> keyExtractor) {
        return new KeyedStreamImpl<>(this, keyExtractor);
    }

    @Override
    public WindowedStream<T> window(Window.Assigner<T> assigner) {
        return new WindowedStreamImpl<>(this, assigner);
    }

    @Override
    public DataStream<T> addOperator(StreamOperator<T> operator) {
        return this;
    }

    @Override
    public DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> strategy) {
        this.watermarkStrategy = strategy;
        return this;
    }

    @Override
    public DataStream<T> addSink(DataSink<T> sink) {
        sinks.add(sink);
        return this;
    }

    @Override
    public List<T> collect() {
        List<T> results = new ArrayList<>();
        DataSink<T> collectorSink = new DataSink<T>() {
            @Override
            public void write(T value) {
                if (value != null) {
                    results.add(value);
                }
            }
        };
        sinks.add(collectorSink);
        execute();
        return results;
    }

    @Override
    public void execute() {
        engine = new ExecutionEngine(
                config.getParallelism(),
                java.time.Duration.ofMillis(config.getWatermarkInterval()),
                config.isEnableAdaptiveWindow(),
                config.isEnableBackpressure()
        );

        engine.start();

        try {
            // 初始化sinks
            for (DataSink<T> sink : sinks) {
                sink.open();
            }

            // 处理数据
            DataSink<T> firstSink = sinks.isEmpty() ? null : sinks.get(0);

            while (source.hasMore()) {
                StreamRecord<?> record = source.nextRecord();
                if (record != null) {
                    // 应用所有转换
                    Object value = record.getValue();
                    boolean filtered = false;
                    for (Function<?, ?> f : transformations) {
                        @SuppressWarnings("unchecked")
                        Function<Object, Object> func = (Function<Object, Object>) f;
                        value = func.apply(value);
                        if (value == null) {
                            filtered = true;
                            break;
                        }
                    }
                    if (!filtered && firstSink != null) {
                        @SuppressWarnings("unchecked")
                        T result = (T) value;
                        try {
                            firstSink.write(result);
                        } catch (Exception e) {
                            log.error("Failed to write to sink: {}", e.getMessage());
                        }
                    }
                }
            }

        } catch (Exception e) {
            log.error("Stream execution failed", e);
        } finally {
            // 关闭sinks
            for (DataSink<T> sink : sinks) {
                try {
                    sink.close();
                } catch (Exception e) {
                    log.error("Sink close failed", e);
                }
            }

            engine.stop();
        }
    }

    @Override
    public StreamResult executeAndGet() {
        long startTime = System.currentTimeMillis();

        execute();

        return StreamResult.builder()
                .startTime(startTime)
                .endTime(System.currentTimeMillis())
                .build();
    }

    // ========== 内部类实现 ==========

    private static class KeyedStreamImpl<T, K> implements KeyedStream<T, K> {
        private final DataStreamImpl<T> parent;
        private final Function<T, K> keyExtractor;

        KeyedStreamImpl(DataStreamImpl<T> parent, Function<T, K> keyExtractor) {
            this.parent = parent;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public Function<T, K> getKeyExtractor() {
            return keyExtractor;
        }

        @Override
        public <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction) {
            return parent.map(t -> aggregateFunction.getResult(aggregateFunction.createAccumulator()));
        }

        @Override
        public DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer) {
            return parent;
        }

        @Override
        public WindowedStream<T> window(Window.Assigner<T> assigner) {
            return new WindowedStreamImpl<>(parent, assigner);
        }

        // 委托方法
        @Override public String getId() { return parent.getId(); }
        @Override public int getParallelism() { return parent.getParallelism(); }
        @Override public <R> DataStream<R> map(Function<T, R> mapper) { return parent.map(mapper); }
        @Override public DataStream<T> filter(Predicate<T> predicate) { return parent.filter(predicate); }
        @Override public <K2> KeyedStream<T, K2> keyBy(Function<T, K2> keyExtractor) { return parent.keyBy(keyExtractor); }
        @Override public DataStream<T> addOperator(StreamOperator<T> operator) { return parent.addOperator(operator); }
        @Override public DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> strategy) { return parent.assignTimestampsAndWatermarks(strategy); }
        @Override public DataStream<T> addSink(DataSink<T> sink) { return parent.addSink(sink); }
        @Override public List<T> collect() { return parent.collect(); }
        @Override public void execute() { parent.execute(); }
        @Override public StreamResult executeAndGet() { return parent.executeAndGet(); }
    }

    private static class WindowedStreamImpl<T> implements WindowedStream<T> {
        private final DataStreamImpl<T> parent;
        @Getter
        private final Window.Assigner<T> assigner;
        @Getter
        private Trigger<T> trigger;
        @Getter
        private long allowedLateness = 0;

        WindowedStreamImpl(DataStreamImpl<T> parent, Window.Assigner<T> assigner) {
            this.parent = parent;
            this.assigner = assigner;
        }

        @Override
        public WindowedStream<T> trigger(Trigger<T> trigger) {
            this.trigger = trigger;
            return this;
        }

        @Override
        public WindowedStream<T> allowedLateness(long lateness) {
            this.allowedLateness = lateness;
            return this;
        }

        @Override
        public WindowedStream<T> sideOutputLateData(String tag) {
            return this;
        }

        @Override
        public <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction) {
            return parent.map(t -> aggregateFunction.getResult(aggregateFunction.createAccumulator()));
        }

        @Override
        public <ACC, R, W> DataStream<W> aggregate(AggregateFunction<T, ACC, R> aggregateFunction, Function<R, W> windowResultMapper) {
            return parent.map(t -> windowResultMapper.apply(aggregateFunction.getResult(aggregateFunction.createAccumulator())));
        }

        @Override
        public DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer) {
            return parent;
        }

        @Override
        public <R> DataStream<R> fold(R initialValue, java.util.function.BiFunction<R, T, R> folder) {
            return parent.map(t -> folder.apply(initialValue, t));
        }

        @Override
        public <R> DataStream<R> apply(Function<Iterable<T>, R> windowFunction) {
            return parent.map(t -> windowFunction.apply(List.of(t)));
        }

        // 委托方法
        @Override public String getId() { return parent.getId(); }
        @Override public int getParallelism() { return parent.getParallelism(); }
        @Override public <R> DataStream<R> map(Function<T, R> mapper) { return parent.map(mapper); }
        @Override public DataStream<T> filter(Predicate<T> predicate) { return parent.filter(predicate); }
        @Override public <K> KeyedStream<T, K> keyBy(Function<T, K> keyExtractor) { return parent.keyBy(keyExtractor); }
        @Override public WindowedStream<T> window(Window.Assigner<T> assigner) { return parent.window(assigner); }
        @Override public DataStream<T> addOperator(StreamOperator<T> operator) { return parent.addOperator(operator); }
        @Override public DataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> strategy) { return parent.assignTimestampsAndWatermarks(strategy); }
        @Override public DataStream<T> addSink(DataSink<T> sink) { return parent.addSink(sink); }
        @Override public List<T> collect() { return parent.collect(); }
        @Override public void execute() { parent.execute(); }
        @Override public StreamResult executeAndGet() { return parent.executeAndGet(); }

    }
}
