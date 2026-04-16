package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 计数累加器
 */
public class CountAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long count;

    public CountAccumulator() {
        this.count = 0;
    }
}
