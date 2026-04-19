package com.kxj.streamingdataengine.storage.lsm;

import java.util.ArrayList;
import java.util.List;

/**
 * 分层Compaction策略
 * 借鉴RocksDB的Leveled Compaction
 */
class LeveledCompaction implements CompactionStrategy {

    private static final int MAX_LEVEL = 7;
    private static final long LEVEL_SIZE_MULTIPLIER = 10;
    private static final double SCORE_THRESHOLD = 1.0;

    @Override
    public <K extends Comparable<K>, V> List<Segment<K, V>> select(List<Segment<K, V>> segments) {
        if (segments.isEmpty()) {
            return List.of();
        }

        // 简单实现：选择最多重叠的level
        // 实际应该维护level信息
        int level = 0;
        long levelSize = 10 * 1024 * 1024; // 10MB基础

        List<Segment<K, V>> currentLevel = new ArrayList<>();
        for (Segment<K, V> segment : segments) {
            if (segment.size() <= levelSize) {
                currentLevel.add(segment);
            } else {
                if (currentLevel.size() > 1) {
                    return currentLevel;
                }
                level++;
                levelSize *= LEVEL_SIZE_MULTIPLIER;
                currentLevel.clear();
                currentLevel.add(segment);
            }
        }

        return currentLevel.size() > 1 ? currentLevel : List.of();
    }
}
