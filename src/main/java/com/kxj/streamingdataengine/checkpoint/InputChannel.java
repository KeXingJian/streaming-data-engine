package com.kxj.streamingdataengine.checkpoint;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 输入通道标识
 * 
 * 在多输入算子中，每个输入源对应一个 InputChannel。
 * 例如：union、join、coGroup 等算子都有多个输入。
 */
@Getter
@ToString
@EqualsAndHashCode
public class InputChannel {
    
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
    
    private final int id;
    private final String name;
    
    public InputChannel(String name) {
        this.id = (int) ID_GENERATOR.incrementAndGet();
        this.name = name;
    }
    
    public InputChannel(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
