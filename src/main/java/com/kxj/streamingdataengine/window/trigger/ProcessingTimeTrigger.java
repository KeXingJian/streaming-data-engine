package com.kxj.streamingdataengine.window.trigger;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.window.Window;

/**
 * 处理时间触发器
 * 当处理时间超过窗口结束时间时触发
 */
public class ProcessingTimeTrigger<T> implements Trigger<T> {

    private static final long serialVersionUID = 1L;

    public static <T> ProcessingTimeTrigger<T> create() {
        return new ProcessingTimeTrigger<>();
    }

    @Override
    public TriggerResult onElement(StreamRecord<T> element, long timestamp, Window window) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window) {
        return time >= window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window) {
        return TriggerResult.CONTINUE;
    }
}
