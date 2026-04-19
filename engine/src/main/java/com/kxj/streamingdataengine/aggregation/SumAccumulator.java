package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 求和累加器（整数/长整数）
 */
public class SumAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long sum;

    public SumAccumulator() {
        this.sum = 0;
    }
}
