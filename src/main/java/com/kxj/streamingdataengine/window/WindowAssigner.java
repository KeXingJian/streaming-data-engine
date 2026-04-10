package com.kxj.streamingdataengine.window;

import com.kxj.streamingdataengine.window.trigger.EventTimeTrigger;
import com.kxj.streamingdataengine.window.trigger.ProcessingTimeTrigger;
import com.kxj.streamingdataengine.window.trigger.Trigger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 窗口分配器实现
 * 支持滚动窗口、滑动窗口、会话窗口
 */
public class WindowAssigner {

    /**
     * 滚动时间窗口
     */
    public static <T> Window.Assigner<T> tumblingTimeWindow(Duration size) {
        return new TumblingTimeWindowAssigner<>(size.toMillis());
    }

    /**
     * 滑动时间窗口
     */
    public static <T> Window.Assigner<T> slidingTimeWindow(Duration size, Duration slide) {
        return new SlidingTimeWindowAssigner<>(size.toMillis(), slide.toMillis());
    }

    /**
     * 会话窗口
     */
    public static <T> Window.Assigner<T> sessionWindow(Duration gap) {
        return new SessionWindowAssigner<>(gap.toMillis());
    }

    /**
     * 全局窗口
     */
    public static <T> Window.Assigner<T> globalWindow() {
        return new GlobalWindowAssigner<>();
    }

    /**
     * 计数滚动窗口
     */
    public static <T> Window.Assigner<T> tumblingCountWindow(long count) {
        return new CountWindowAssigner<>(count, count);
    }

    /**
     * 计数滑动窗口
     */
    public static <T> Window.Assigner<T> slidingCountWindow(long size, long slide) {
        return new CountWindowAssigner<>(size, slide);
    }

    /**
     * 滚动时间窗口分配器
     */
    private static class TumblingTimeWindowAssigner<T> implements Window.Assigner<T> {
        private final long size;

        TumblingTimeWindowAssigner(long size) {
            this.size = size;
        }

        @Override
        public List<Window> assignWindows(T element, long timestamp) {
            long start = timestamp - (timestamp % size);
            return List.of(new Window.TimeWindow(start, start + size));
        }

        @Override
        public Trigger<T> getDefaultTrigger() {
            return EventTimeTrigger.create();
        }
    }

    /**
     * 滑动时间窗口分配器
     */
    private static class SlidingTimeWindowAssigner<T> implements Window.Assigner<T> {
        private final long size;
        private final long slide;

        SlidingTimeWindowAssigner(long size, long slide) {
            this.size = size;
            this.slide = slide;
        }

        @Override
        public List<Window> assignWindows(T element, long timestamp) {
            List<Window> windows = new ArrayList<>();
            long start = timestamp - (timestamp % slide);

            // 一个事件可能属于多个窗口
            for (long windowStart = start; windowStart > timestamp - size; windowStart -= slide) {
                windows.add(new Window.TimeWindow(windowStart, windowStart + size));
            }

            return windows;
        }

        @Override
        public Trigger<T> getDefaultTrigger() {
            return EventTimeTrigger.create();
        }
    }

    /**
     * 会话窗口分配器
     */
    private static class SessionWindowAssigner<T> implements Window.Assigner<T> {
        private final long gap;

        SessionWindowAssigner(long gap) {
            this.gap = gap;
        }

        @Override
        public List<Window> assignWindows(T element, long timestamp) {
            return List.of(new Window.SessionWindow(timestamp, timestamp + gap));
        }

        @Override
        public Trigger<T> getDefaultTrigger() {
            return EventTimeTrigger.create();
        }
    }

    /**
     * 全局窗口分配器
     */
    private static class GlobalWindowAssigner<T> implements Window.Assigner<T> {
        @Override
        public List<Window> assignWindows(T element, long timestamp) {
            return List.of(Window.GlobalWindow.get());
        }

        @Override
        public Trigger<T> getDefaultTrigger() {
            return ProcessingTimeTrigger.create();
        }
    }

    /**
     * 计数窗口分配器
     */
    private static class CountWindowAssigner<T> implements Window.Assigner<T> {
        private final long size;
        private final long slide;

        CountWindowAssigner(long size, long slide) {
            this.size = size;
            this.slide = slide;
        }

        @Override
        public List<Window> assignWindows(T element, long timestamp) {
            // 计数窗口使用全局窗口+触发器实现
            return List.of(Window.GlobalWindow.get());
        }

        @Override
        public Trigger<T> getDefaultTrigger() {
            return ProcessingTimeTrigger.create();
        }
    }
}
