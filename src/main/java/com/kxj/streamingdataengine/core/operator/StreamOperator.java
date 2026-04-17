package com.kxj.streamingdataengine.core.operator;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;

import java.util.List;

/**
 * 流算子接口
 * 借鉴Flink的Operator设计
 */
public interface StreamOperator<T> {

    /**
     * 算子名称
     */
    String getName();

    /**
     * 处理单条记录
     */
    List<StreamRecord<T>> processElement(StreamRecord<T> record);

    /**
     * 处理Watermark
     * @return 算子因Watermark推进而发射的记录列表
     */
    List<StreamRecord<T>> processWatermark(Watermark watermark);

    /**
     * 打开算子
     */
    default void open() {}

    /**
     * 关闭算子
     */
    default void close() {}

    /**
     * 获取算子状态
     */
    default OperatorState snapshotState() {
        return null;
    }

    /**
     * 恢复算子状态
     */
    default void restoreState(OperatorState state) {}

    /**
     * 算子状态
     */
    interface OperatorState {
        byte[] serialize();
        void deserialize(byte[] data);
    }
}
