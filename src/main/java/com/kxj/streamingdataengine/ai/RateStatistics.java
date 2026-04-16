package com.kxj.streamingdataengine.ai;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 速率统计器
 * 基于AtomicLong和毫秒时间戳计算每秒速率
 */
public class RateStatistics {
    private final AtomicLong eventCount = new AtomicLong(0);
    private volatile long lastCountTime = System.currentTimeMillis();
    /**
     * -- GETTER --
     *  获取当前每秒事件速率
     */
    @Getter
    private volatile double currentRate = 0;

    /**
     * 记录一个事件并更新速率
     */
    public void update() {
        eventCount.incrementAndGet();

        long now = System.currentTimeMillis();
        if (now - lastCountTime >= 1000) {
            long count = eventCount.getAndSet(0);
            currentRate = count * 1000.0 / (now - lastCountTime);
            lastCountTime = now;
        }
    }

}
