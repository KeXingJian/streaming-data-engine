package com.kxj.streamingdataengine.ai;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 严重程度级别枚举
 * 统一用于异常检测和背压控制等场景
 */
@AllArgsConstructor
@Getter
public enum SeverityLevel {
    NORMAL(0),
    MEDIUM(1),
    HIGH(2),
    CRITICAL(3);

    private final int severity;
}
