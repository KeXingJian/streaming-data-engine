package com.kxj.streamingdataengine.window.trigger;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.window.Window;

import java.io.Serial;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 计数触发器
 * 当窗口内元素数量达到阈值时触发
 */
public class CountTrigger<T> implements Trigger<T> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long maxCount;
    private final ConcurrentHashMap<Window, AtomicLong> counts = new ConcurrentHashMap<>();

    public CountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    public static <T> CountTrigger<T> of(long maxCount) {
        return new CountTrigger<>(maxCount);
    }

    @Override
    public TriggerResult onElement(StreamRecord<T> element, long timestamp, Window window) {
        long count = counts.computeIfAbsent(window, w -> new AtomicLong(0)).incrementAndGet();
        if (count >= maxCount) {
            counts.remove(window);
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window) {
        return TriggerResult.CONTINUE;
    }
}
