package com.kxj.streamingdataengine.window.trigger;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.core.model.Watermark;
import com.kxj.streamingdataengine.window.Window;

import java.io.Serializable;

/**
 * 窗口触发器
 * 决定何时触发窗口计算
 */
public interface Trigger<T> extends Serializable {

    /**
     * 每个元素进入窗口时调用
     * @return 触发结果
     */
    TriggerResult onElement(StreamRecord<T> element, long timestamp, Window window);

    /**
     * 处理时间到达时调用
     */
    TriggerResult onProcessingTime(long time, Window window);

    /**
     * 事件时间到达时调用（Watermark推进）
     */
    TriggerResult onEventTime(long time, Window window);

    /**
     * 是否可以合并（用于会话窗口）
     */
    default boolean canMerge() {
        return false;
    }

    /**
     * 触发器结果
     */
    enum TriggerResult {
        /**
         * 不触发，保留窗口
         */
        CONTINUE(false, false),

        /**
         * 触发计算并保留窗口
         */
        FIRE_AND_PURGE(true, true),

        /**
         * 触发计算但保留窗口（支持延迟触发）
         */
        FIRE(true, false),

        /**
         * 触发计算并清理窗口
         */
        PURGE(false, true);

        private final boolean fire;
        private final boolean purge;

        TriggerResult(boolean fire, boolean purge) {
            this.fire = fire;
            this.purge = purge;
        }

        public boolean isFire() {
            return fire;
        }

        public boolean isPurge() {
            return purge;
        }
    }
}
