package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 可变包装器（用于 max/min 聚合）
 */
public class Holder<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    T value;

    public Holder(T value) {
        this.value = value;
    }
}
