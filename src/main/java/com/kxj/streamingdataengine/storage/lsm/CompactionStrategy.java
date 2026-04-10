package com.kxj.streamingdataengine.storage.lsm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Compaction策略接口
 */
interface CompactionStrategy {
    /**
     * 选择需要合并的Segments
     */
    <K extends Comparable<K>, V> List<Segment<K, V>> select(List<Segment<K, V>> segments);
}

/**
 * 大小分层Compaction策略
 * 借鉴Cassandra的Size-Tiered Compaction
 */
class SizeTieredCompaction implements CompactionStrategy {

    private static final double SIZE_THRESHOLD = 1.5; // 大小比阈值
    private static final int MIN_COMPACTION_SIZE = 4; // 最少合并数量

    @Override
    public <K extends Comparable<K>, V> List<Segment<K, V>> select(List<Segment<K, V>> segments) {
        if (segments.size() < MIN_COMPACTION_SIZE) {
            return List.of();
        }

        // 按大小分组
        List<Segment<K, V>> candidate = new ArrayList<>();
        long minSize = Long.MAX_VALUE;

        for (Segment<K, V> segment : segments) {
            if (candidate.isEmpty()) {
                candidate.add(segment);
                minSize = segment.size();
            } else {
                // 检查是否符合大小分层条件
                if ((double) segment.size() / minSize <= SIZE_THRESHOLD) {
                    candidate.add(segment);
                } else {
                    // 开始新的分组
                    if (candidate.size() >= MIN_COMPACTION_SIZE) {
                        return candidate;
                    }
                    candidate.clear();
                    candidate.add(segment);
                    minSize = segment.size();
                }
            }
        }

        return candidate.size() >= MIN_COMPACTION_SIZE ? candidate : List.of();
    }
}

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
