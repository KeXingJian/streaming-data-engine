package com.kxj.streamingdataengine.window;

import com.kxj.streamingdataengine.window.trigger.Trigger;
import lombok.Getter;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 窗口抽象
 * 借鉴Flink的Window设计
 */
@Getter
@ToString
public abstract class Window implements Comparable<Window>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    protected final long start; // 窗口开始时间

    protected final long end; // 窗口结束时间（不包含）

    protected Window(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * 获取最大时间戳（结束时间-1）
     */
    public long maxTimestamp() {
        return end - 1;
    }

    /**
     * 判断时间戳是否在窗口内
     */
    public boolean contains(long timestamp) {
        return timestamp >= start && timestamp < end;
    }

    /**
     * 判断窗口是否与给定时间范围相交
     */
    public boolean intersects(long otherStart, long otherEnd) {
        return this.start < otherEnd && this.end > otherStart;
    }

    /**
     * 计算窗口与另一窗口的并集
     */
    public Window cover(Window other) {
        return new TimeWindow(
                Math.min(this.start, other.start),
                Math.max(this.end, other.end)
        );
    }

    @Override
    public int compareTo(Window other) {
        int cmp = Long.compare(this.start, other.start);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.end, other.end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return start == window.start && end == window.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    /**
     * 时间窗口
     */
    public static class TimeWindow extends Window {
        public TimeWindow(long start, long end) {
            super(start, end);
        }
    }

    /**
     * 全局窗口（只有一个窗口）
     */
    public static class GlobalWindow extends Window {
        private static final GlobalWindow INSTANCE = new GlobalWindow();

        private GlobalWindow() {
            super(0, Long.MAX_VALUE);
        }

        public static GlobalWindow get() {
            return INSTANCE;
        }

        @Override
        public boolean contains(long timestamp) {
            return true;
        }
    }

    /**
     * 会话窗口
     */
    public static class SessionWindow extends Window {
        public SessionWindow(long start, long end) {
            super(start, end);
        }
    }

    /**
     * 窗口分配器接口
     */
    public interface Assigner<T> {
        /**
         * 分配窗口
         */
        java.util.List<Window> assignWindows(T element, long timestamp);

        /**
         * 获取默认触发器
         */
        Trigger<T> getDefaultTrigger();
    }
}
