package com.kxj.streamingdataengine.stream;

import com.kxj.streamingdataengine.aggregation.AggregateFunction;
import com.kxj.streamingdataengine.core.model.DataSink;
import com.kxj.streamingdataengine.core.model.DataSource;
import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.StreamResult;
import com.kxj.streamingdataengine.core.model.DataStream;
import com.kxj.streamingdataengine.core.model.KeyedStream;
import com.kxj.streamingdataengine.core.model.WindowedStream;
import com.kxj.streamingdataengine.core.operator.StreamOperator;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.operator.TransformOperator;
import com.kxj.streamingdataengine.window.Window;
import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * DataStream实现
 */
@Slf4j
public class DataStreamImpl<T> implements DataStream<T> {

    private final String jobName;
    private final StreamConfig config = new StreamConfig();
    private final DataSource<T> source;

    // 转换链：存储从source到当前类型的所有转换函数
    private final List<Function<?, ?>> transformations;

    protected final List<DataSink<T>> sinks = new CopyOnWriteArrayList<>();

    // 构造源流
    public DataStreamImpl(String jobName, DataSource<T> source) {
        this.jobName = jobName;

        this.source = source;
        this.transformations = new ArrayList<>();
    }

    // 构造转换流
    private DataStreamImpl(String jobName, DataSource<?> source,
                           List<Function<?, ?>> transformations) {
        this.jobName = jobName;
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
        // [kxj: map转换采用不可变设计，复制转换链并添加新转换，返回新流实例]
        List<Function<?, ?>> newTransformations = copyTransformations();
        newTransformations.add(mapper);
        log.debug("[kxj: map转换] 当前转换链长度={}, 新增map转换", newTransformations.size());
        return new DataStreamImpl<>(jobName + "_map", source, newTransformations);
    }

    @Override
    public DataStream<T> filter(Predicate<T> predicate) {
        List<Function<?, ?>> newTransformations = copyTransformations();
        newTransformations.add((Function<T, T>) v -> predicate.test(v) ? v : null);
        return new DataStreamImpl<>(jobName + "_filter", source, newTransformations);
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
    public DataStream<T> addSink(DataSink<T> sink) {
        sinks.add(sink);
        return this;
    }

    @Override
    public List<T> collect() {
        List<T> results = new ArrayList<>();
        DataSink<T> collectorSink = value -> {
            if (value != null) {
                results.add(value);
            }
        };
        sinks.add(collectorSink);
        execute();
        return results;
    }

    @Override
    public void execute() {
        // [kxj: 执行引擎初始化，根据配置启动并行度、自适应窗口和背压控制]
        ExecutionEngine engine = new ExecutionEngine(
                config.getParallelism(),
                Duration.ofMillis(config.getWatermarkInterval()),
                config.isEnableAdaptiveWindow(),
                config.isEnableBackpressure()
        );

        log.info("[kxj: 执行流] jobName={}, 转换链长度={}, sinks数量={}", jobName, transformations.size(), sinks.size());
        engine.start();

        try {
            // 初始化sinks
            for (DataSink<T> sink : sinks) {
                sink.open();
            }

            // [kxj: 将转换函数链封装为StreamOperator，供执行引擎统一调度]
            List<StreamOperator<T>> operators = new ArrayList<>();
            for (Function<?, ?> f : transformations) {
                @SuppressWarnings("unchecked")
                Function<Object, Object> func = (Function<Object, Object>) f;
                operators.add(new TransformOperator<>(func));
            }

            // [kxj: 多sink聚合为复合sink，适配engine单sink接口]
            DataSink<T> combinedSink = sinks.isEmpty() ? null : value -> {
                for (DataSink<T> sink : sinks) {
                    try {
                        sink.write(value);
                    } catch (Exception e) {
                        log.error("[kxj: Sink写入失败] 原因: {}", e.getMessage());
                    }
                }
            };

            int processedCount = 0;
            int filteredCount = 0;
            // [kxj: 从source读取数据，交由engine处理，应用算子链并写入sink]
            while (source.hasMore()) {
                StreamRecord<T> record = source.nextRecord();
                if (record != null) {
                    List<StreamRecord<T>> results = engine.processRecord(record, operators, combinedSink);
                    if (results.isEmpty()) {
                        filteredCount++;
                    } else {
                        processedCount += results.size();
                    }
                }
            }
            log.info("[kxj: 执行完成] 处理记录数={}, 过滤记录数={}", processedCount, filteredCount);

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

    private List<Function<?, ?>> copyTransformations() {
        return new ArrayList<>(transformations);
    }

    private static <R> void writeToSinks(List<R> results, List<DataSink<R>> sinks) {
        for (DataSink<R> sink : sinks) {
            try (sink) {

                sink.open();
                for (R r : results) {
                    sink.write(r);
                }

            } catch (Exception e) {
                log.error("Sink close failed", e);
            }
        }
    }

    // ========== 内部类实现 ==========

    private record KeyedStreamImpl<T, K>(DataStreamImpl<T> parent,
                                         Function<T, K> keyExtractor) implements KeyedStream<T, K> {

        @Override
        public <ACC, R> DataStream<R> aggregate(AggregateFunction<T, ACC, R> aggregateFunction) {
            return new DataStreamImpl<>(parent.jobName + "_keyedAggregate", parent.source, parent.transformations) {
                @Override
                public List<R> collect() {
                    List<T> inputs = parent.collect();
                    Map<K, ACC> accumulators = new HashMap<>();
                    for (T item : inputs) {
                        K key = keyExtractor.apply(item);
                        ACC acc = accumulators.computeIfAbsent(key, k -> aggregateFunction.createAccumulator());
                        aggregateFunction.add(item, acc);
                    }
                    List<R> results = new ArrayList<>();
                    for (ACC acc : accumulators.values()) {
                        results.add(aggregateFunction.getResult(acc));
                    }
                    return results;
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        @Override
        public DataStream<T> reduce(BinaryOperator<T> reducer) {
            return new DataStreamImpl<>(parent.jobName + "_keyedReduce", parent.source, parent.transformations) {
                @Override
                public List<T> collect() {
                    List<T> inputs = parent.collect();
                    Map<K, T> results = new HashMap<>();
                    for (T item : inputs) {
                        K key = keyExtractor.apply(item);
                        results.merge(key, item, reducer);
                    }
                    return new ArrayList<>(results.values());
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        @Override
        public WindowedStream<T> window(Window.Assigner<T> assigner) {
            return new WindowedStreamImpl<>(parent, assigner);
        }

        // 委托方法
        @Override
        public String getId() {
            return parent.getId();
        }

        @Override
        public int getParallelism() {
            return parent.getParallelism();
        }

        @Override
        public <R> DataStream<R> map(Function<T, R> mapper) {
            return parent.map(mapper);
        }

        @Override
        public DataStream<T> filter(Predicate<T> predicate) {
            return parent.filter(predicate);
        }

        @Override
        public <K2> KeyedStream<T, K2> keyBy(Function<T, K2> keyExtractor) {
            return parent.keyBy(keyExtractor);
        }

        @Override
        public DataStream<T> addSink(DataSink<T> sink) {
            return parent.addSink(sink);
        }

        @Override
        public List<T> collect() {
            return parent.collect();
        }

        @Override
        public void execute() {
            parent.execute();
        }

        @Override
        public StreamResult executeAndGet() {
            return parent.executeAndGet();
        }
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
            return new DataStreamImpl<>(parent.jobName + "_windowAggregate", parent.source, parent.transformations) {
                @Override
                public List<R> collect() {
                    List<T> inputs = parent.collect();
                    Map<Window, ACC> accumulators = new HashMap<>();
                    long timestamp = System.currentTimeMillis();
                    for (T item : inputs) {
                        List<Window> windows = assigner.assignWindows(item, timestamp);
                        for (Window window : windows) {
                            ACC acc = accumulators.computeIfAbsent(window, w -> aggregateFunction.createAccumulator());
                            aggregateFunction.add(item, acc);
                        }
                    }
                    List<R> results = new ArrayList<>();
                    for (ACC acc : accumulators.values()) {
                        results.add(aggregateFunction.getResult(acc));
                    }
                    return results;
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        @Override
        public <ACC, R, W> DataStream<W> aggregate(AggregateFunction<T, ACC, R> aggregateFunction, Function<R, W> windowResultMapper) {
            return aggregate(aggregateFunction).map(windowResultMapper);
        }

        @Override
        public DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer) {
            return new DataStreamImpl<>(parent.jobName + "_windowReduce", parent.source, parent.transformations) {
                @Override
                public List<T> collect() {
                    List<T> inputs = parent.collect();
                    Map<Window, T> results = new HashMap<>();
                    long timestamp = System.currentTimeMillis();
                    for (T item : inputs) {
                        List<Window> windows = assigner.assignWindows(item, timestamp);
                        for (Window window : windows) {
                            results.merge(window, item, reducer);
                        }
                    }
                    return new ArrayList<>(results.values());
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        @Override
        public <R> DataStream<R> fold(R initialValue, java.util.function.BiFunction<R, T, R> folder) {
            return new DataStreamImpl<>(parent.jobName + "_windowFold", parent.source, parent.transformations) {
                @Override
                public List<R> collect() {
                    List<T> inputs = parent.collect();
                    Map<Window, R> accMap = new HashMap<>();
                    long timestamp = System.currentTimeMillis();
                    for (T item : inputs) {
                        List<Window> windows = assigner.assignWindows(item, timestamp);
                        for (Window window : windows) {
                            accMap.put(window, folder.apply(accMap.getOrDefault(window, initialValue), item));
                        }
                    }
                    return new ArrayList<>(accMap.values());
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        @Override
        public <R> DataStream<R> apply(Function<Iterable<T>, R> windowFunction) {
            return new DataStreamImpl<>(parent.jobName + "_windowApply", parent.source, parent.transformations) {
                @Override
                public List<R> collect() {
                    List<T> inputs = parent.collect();
                    Map<Window, List<T>> groups = new HashMap<>();
                    long timestamp = System.currentTimeMillis();
                    for (T item : inputs) {
                        List<Window> windows = assigner.assignWindows(item, timestamp);
                        for (Window window : windows) {
                            groups.computeIfAbsent(window, w -> new ArrayList<>()).add(item);
                        }
                    }
                    List<R> results = new ArrayList<>();
                    for (List<T> group : groups.values()) {
                        results.add(windowFunction.apply(group));
                    }
                    return results;
                }

                @Override
                public void execute() {
                    writeToSinks(collect(), sinks);
                }
            };
        }

        // 委托方法
        @Override
        public String getId() {
            return parent.getId();
        }

        @Override
        public int getParallelism() {
            return parent.getParallelism();
        }

        @Override
        public <R> DataStream<R> map(Function<T, R> mapper) {
            return parent.map(mapper);
        }

        @Override
        public DataStream<T> filter(Predicate<T> predicate) {
            return parent.filter(predicate);
        }

        @Override
        public <K> KeyedStream<T, K> keyBy(Function<T, K> keyExtractor) {
            return parent.keyBy(keyExtractor);
        }

        @Override
        public WindowedStream<T> window(Window.Assigner<T> assigner) {
            return parent.window(assigner);
        }

        @Override
        public DataStream<T> addSink(DataSink<T> sink) {
            return parent.addSink(sink);
        }

        @Override
        public List<T> collect() {
            return parent.collect();
        }

        @Override
        public void execute() {
            parent.execute();
        }

        @Override
        public StreamResult executeAndGet() {
            return parent.executeAndGet();
        }

    }
}
