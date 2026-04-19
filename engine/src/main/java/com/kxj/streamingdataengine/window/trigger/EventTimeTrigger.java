package com.kxj.streamingdataengine.window.trigger;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.window.Window;

import java.io.Serial;

/**
 * 事件时间触发器
 * 当Watermark超过窗口结束时间时触发
 */
public class EventTimeTrigger<T> implements Trigger<T> {

    @Serial
    private static final long serialVersionUID = 1L;

    public static <T> EventTimeTrigger<T> create() {
        return new EventTimeTrigger<>();
    }

    @Override
    public TriggerResult onElement(StreamRecord<T> element, long timestamp, Window window) {
        if (window.maxTimestamp() <= timestamp) {
            // 元素已经迟到
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window) {
        return time >= window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
    }
}
