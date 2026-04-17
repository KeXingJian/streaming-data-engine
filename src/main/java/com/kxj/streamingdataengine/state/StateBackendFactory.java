package com.kxj.streamingdataengine.state;

import com.kxj.streamingdataengine.state.lsm.LSMTreeStateBackend;
import com.kxj.streamingdataengine.state.memory.MemoryStateBackend;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 状态后端工厂
 * 用于创建不同类型的状态后端实例
 */
@Slf4j
public class StateBackendFactory {
    
    /**
     * 创建内存状态后端
     */
    public static StateBackend createMemoryBackend() {
        log.info("[kxj: 创建内存状态后端]");
        return new MemoryStateBackend();
    }
    
    /**
     * 创建 LSM-Tree 状态后端
     */
    public static StateBackend createLSMTreeBackend(String path) {
        Path basePath = Paths.get(path);
        log.info("[kxj: 创建 LSM-Tree 状态后端 - path={}]", basePath);
        return new LSMTreeStateBackend(basePath);
    }
    
    /**
     * 创建 LSM-Tree 状态后端（默认路径）
     */
    public static StateBackend createLSMTreeBackend() {
        String defaultPath = System.getProperty("java.io.tmpdir") + "/streaming-state";
        return createLSMTreeBackend(defaultPath);
    }
    
    /**
     * 根据配置创建状态后端
     */
    public static StateBackend createBackend(StateBackendType type, String... params) {
        return switch (type) {
            case MEMORY -> createMemoryBackend();
            case LSM_TREE -> params.length > 0 ? createLSMTreeBackend(params[0]) : createLSMTreeBackend();
        };
    }
    
    public enum StateBackendType {
        MEMORY,
        LSM_TREE
    }
}
