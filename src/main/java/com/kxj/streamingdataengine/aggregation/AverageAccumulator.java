package com.kxj.streamingdataengine.aggregation;

import java.io.Serial;
import java.io.Serializable;

/**
 * 平均值累加器
 */
public class AverageAccumulator implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    long count;
    double sum;

    public AverageAccumulator() {
        this.count = 0;
        this.sum = 0.0;
    }

    public void add(double value) {
        count++;
        sum += value;
    }

    public double getAverage() {
        return count == 0 ? 0.0 : sum / count;
    }

    public void merge(AverageAccumulator other) {
        this.count += other.count;
        this.sum += other.sum;
    }
}
