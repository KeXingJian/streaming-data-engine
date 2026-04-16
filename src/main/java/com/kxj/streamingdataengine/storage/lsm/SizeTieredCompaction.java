package com.kxj.streamingdataengine.storage.lsm;

import java.util.ArrayList;
import java.util.List;

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
