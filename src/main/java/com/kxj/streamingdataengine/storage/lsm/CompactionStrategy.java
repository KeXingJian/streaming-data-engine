package com.kxj.streamingdataengine.storage.lsm;

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
