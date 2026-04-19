package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 浮点数求和累加器
 */
public class DoubleSumAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    double sum;

    public DoubleSumAccumulator() {
        this.sum = 0.0;
    }
}
