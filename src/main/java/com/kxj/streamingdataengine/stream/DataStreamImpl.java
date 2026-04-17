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
import com.kxj.streamingdataengine.operator.KeyedAggregateOperator;
import com.kxj.streamingdataengine.operator.KeyedReduceOperator;
import com.kxj.streamingdataengine.operator.TransformOperator;
import com.kxj.streamingdataengine.operator.WindowAggregateOperator;
import com.kxj.streamingdataengine.operator.WindowApplyOperator;
import com.kxj.streamingdataengine.operator.WindowFoldOperator;
import com.kxj.streamingdataengine.operator.WindowReduceOperator;
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
    private StreamConfig config = new StreamConfig();
    private final DataSource<T> source;

    // 转换链：存储从source到当前类型的所有转换函数
    private final List<Function<?, ?>> transformations;

    // 自定义算子链：流式有状态算子（keyed/window）在execute时追加到TransformOperator之后
    private final List<StreamOperator<?>> customOperators;

    protected final List<DataSink<T>> sinks = new CopyOnWriteArrayList<>();

    // 构造源流
    public DataStreamImpl(String jobName, DataSource<T> source) {
        this.jobName = jobName;
        this.source = source;
        this.transformations = new ArrayList<>();
        this.customOperators = new ArrayList<>();
    }

    // 构造带配置的源流（测试用）
    public DataStreamImpl(String jobName, DataSource<T> source, StreamConfig config) {
        this.jobName = jobName;
        this.source = source;
        this.config = config;
        this.transformations = new ArrayList<>();
        this.customOperators = new ArrayList<>();
    }

    // 构造转换流
    private DataStreamImpl(String jobName, DataSource<?> source,
                           List<Function<?, ?>> transformations) {
        this(jobName, source, transformations, new ArrayList<>());
    }

    // 构造带自定义算子的流
    private DataStreamImpl(String jobName, DataSource<?> source,
                           List<Function<?, ?>> transformations,
                           List<StreamOperator<?>> customOperators) {
        this.jobName = jobName;
        this.source = (DataSource<T>) source;
        this.transformations = new ArrayList<>(transformations);
        this.customOperators = new ArrayList<>(customOperators);
    }

    // 构造带自定义算子和配置的流（内部复制用）
    private DataStreamImpl(String jobName, DataSource<?> source,
                           List<Function<?, ?>> transformations,
                           List<StreamOperator<?>> customOperators,
                           StreamConfig config) {
        this.jobName = jobName;
        this.source = (DataSource<T>) source;
        this.transformations = new ArrayList<>(transformations);
        this.customOperators = new ArrayList<>(customOperators);
        this.config = config;
    }

    @Override
    public String getId() {
        return jobName;
    }

    @Override
    public int getParallelism() {
        return config.getParallelism();
    }

    /**
     * 返回携带新配置的流实例（不可变复制）
     */
    public DataStreamImpl<T> withConfig(StreamConfig newConfig) {
        return new DataStreamImpl<>(jobName, source, transformations, customOperators, newConfig);
    }

    @Override
    public <R> DataStream<R> map(Function<T, R> mapper) {
        // [kxj: map转换采用不可变设计，复制转换链并添加新转换，返回新流实例]
        List<Function<?, ?>> newTransformations = copyTransformations();
        newTransformations.add(mapper);
        log.debug("[kxj: map转换] 当前转换链长度={}, 新增map转换", newTransformations.size());
        return new DataStreamImpl<>(jobName + "_map", source, newTransformations, customOperators, config);
    }

    @Override
    public DataStream<T> filter(Predicate<T> predicate) {
        List<Function<?, ?>> newTransformations = copyTransformations();
        newTransformations.add((Function<T, T>) v -> predicate.test(v) ? v : null);
        return new DataStreamImpl<>(jobName + "_filter", source, newTransformations, customOperators, config);
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

        ExecutionEngine.Pipeline<T> pipeline = null;
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
            // [kxj: 追加自定义流式算子到Pipeline]
            for (StreamOperator<?> custom : customOperators) {
                @SuppressWarnings("unchecked")
                StreamOperator<T> op = (StreamOperator<T>) custom;
                operators.add(op);
            }

            // [kxj: 多sink聚合为复合sink，适配engine单sink接口]
            @SuppressWarnings("unchecked")
            DataSink<T> combinedSink = sinks.isEmpty() ? null : value -> {
                for (DataSink<T> sink : sinks) {
                    try {
                        sink.write(value);
                    } catch (Exception e) {
                        log.error("[kxj: Sink写入失败] 原因: {}", e.getMessage());
                    }
                }
            };

            // [kxj: 注册活跃算子链与sink到引擎，供Watermark全局驱动]
            engine.setPipeline(operators, combinedSink);

            if (config.isEnableBackpressure()) {
                // [kxj: 启用管道级自然背压 - BlockingQueue + 虚拟线程消费者]
                pipeline = engine.createPipeline(operators, combinedSink, config.getBufferSize());
                while (source.hasMore()) {
                    StreamRecord<T> record = source.nextRecord();
                    if (record != null) {
                        pipeline.submit(record);
                    }
                }
            } else {
                // [kxj: 从source读取数据，交由engine处理，应用算子链并写入sink]
                while (source.hasMore()) {
                    StreamRecord<T> record = source.nextRecord();
                    if (record != null) {
                        engine.processRecord(record, operators, combinedSink);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Stream execution failed", e);
        } finally {
            if (pipeline != null) {
                try {
                    pipeline.complete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("[kxj: 执行完成] 处理记录数={}, 过滤记录数={}", engine.getProcessedCount(), engine.getFilteredCount());

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
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            long ttlMs = parent.config.getStateTtl().toMillis();
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new KeyedAggregateOperator<T, K, ACC, R>(keyExtractor, aggregateFunction, ttlMs);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_keyedAggregate", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
        }

        @Override
        public DataStream<T> reduce(BinaryOperator<T> reducer) {
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            long ttlMs = parent.config.getStateTtl().toMillis();
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new KeyedReduceOperator<T, K>(keyExtractor, reducer, ttlMs);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_keyedReduce", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
        }

        @Override
        public WindowedStream<T> window(Window.Assigner<T> assigner) {
            return new WindowedStreamImpl<>(parent, assigner, keyExtractor);
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
        private final Function<T, ?> keyExtractor;

        WindowedStreamImpl(DataStreamImpl<T> parent, Window.Assigner<T> assigner) {
            this(parent, assigner, null);
        }

        WindowedStreamImpl(DataStreamImpl<T> parent, Window.Assigner<T> assigner, Function<T, ?> keyExtractor) {
            this.parent = parent;
            this.assigner = assigner;
            this.keyExtractor = keyExtractor;
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
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new WindowAggregateOperator<T, ACC, R>(
                    assigner, trigger, aggregateFunction, keyExtractor, allowedLateness);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_windowAggregate", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
        }

        @Override
        public <ACC, R, W> DataStream<W> aggregate(AggregateFunction<T, ACC, R> aggregateFunction, Function<R, W> windowResultMapper) {
            return aggregate(aggregateFunction).map(windowResultMapper);
        }

        @Override
        public DataStream<T> reduce(java.util.function.BinaryOperator<T> reducer) {
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new WindowReduceOperator<T>(
                    assigner, trigger, reducer, keyExtractor, allowedLateness);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_windowReduce", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
        }

        @Override
        public <R> DataStream<R> fold(R initialValue, java.util.function.BiFunction<R, T, R> folder) {
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new WindowFoldOperator<T, R>(
                    assigner, trigger, initialValue, folder, keyExtractor, allowedLateness);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_windowFold", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
        }

        @Override
        public <R> DataStream<R> apply(Function<Iterable<T>, R> windowFunction) {
            List<StreamOperator<?>> newCustomOperators = new ArrayList<>(parent.customOperators);
            @SuppressWarnings("unchecked")
            StreamOperator<?> op = new WindowApplyOperator<T, R>(
                    assigner, trigger, windowFunction, keyExtractor, allowedLateness);
            newCustomOperators.add(op);
            return new DataStreamImpl<>(parent.jobName + "_windowApply", parent.source,
                    parent.transformations, newCustomOperators, parent.config);
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
