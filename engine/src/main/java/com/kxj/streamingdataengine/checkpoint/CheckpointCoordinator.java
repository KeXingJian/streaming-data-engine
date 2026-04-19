package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.state.StateBackend;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checkpoint 协调器
 * 
 * 负责：
 * 1. 定时触发 Checkpoint
 * 2. 跟踪 Checkpoint 进度
 * 3. 管理 Checkpoint 生命周期
 * 4. 处理 Checkpoint 超时和失败
 */
@Slf4j
public class CheckpointCoordinator {
    
    private final CheckpointConfig config;
    private final StateBackend stateBackend;
    private final CheckpointListener listener;
    
    private final ScheduledExecutorService scheduler;
    private final ExecutorService checkpointExecutor;
    
    private final AtomicLong checkpointCounter;
    private final Map<Long, Checkpoint> pendingCheckpoints;
    private final Map<Long, Checkpoint> completedCheckpoints;
    
    private volatile boolean isRunning;
    private volatile ScheduledFuture<?> scheduledCheckpoint;
    
    public CheckpointCoordinator(CheckpointConfig config, StateBackend stateBackend, 
                                CheckpointListener listener) {
        this.config = config;
        this.stateBackend = stateBackend;
        this.listener = listener;
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "checkpoint-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        this.checkpointExecutor = Executors.newFixedThreadPool(
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "checkpoint-worker");
                t.setDaemon(true);
                return t;
            }
        );
        
        this.checkpointCounter = new AtomicLong(0);
        this.pendingCheckpoints = new ConcurrentHashMap<>();
        this.completedCheckpoints = new ConcurrentHashMap<>();
        this.isRunning = false;
    }
    
    /**
     * 启动 Checkpoint 协调器
     */
    public void start() {
        if (!config.isEnabled()) {
            log.info("[kxj: Checkpoint 已禁用]");
            return;
        }
        
        if (isRunning) {
            return;
        }
        
        isRunning = true;
        log.info("[kxj: CheckpointCoordinator 启动] interval={}ms", config.getInterval().toMillis());
        
        // 定时触发 Checkpoint（立即触发第一次，之后按间隔触发）
        scheduledCheckpoint = scheduler.scheduleAtFixedRate(
            this::triggerCheckpoint,
            0,  // initialDelay: 立即触发第一次
            config.getInterval().toMillis(),
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * 停止 Checkpoint 协调器
     */
    public void stop() {
        isRunning = false;
        
        if (scheduledCheckpoint != null) {
            scheduledCheckpoint.cancel(false);
        }
        
        scheduler.shutdown();
        checkpointExecutor.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!checkpointExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                checkpointExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        log.info("[kxj: CheckpointCoordinator 已停止]");
    }
    
    /**
     * 触发新的 Checkpoint
     */
    public synchronized void triggerCheckpoint() {
        if (!isRunning || pendingCheckpoints.size() >= config.getMaxConcurrentCheckpoints()) {
            return;
        }
        
        long checkpointNumber = checkpointCounter.incrementAndGet();
        String checkpointId = UUID.randomUUID().toString();
        
        Checkpoint checkpoint = new Checkpoint(checkpointId, checkpointNumber);
        pendingCheckpoints.put(checkpointNumber, checkpoint);
        
        log.info("[kxj: 触发 Checkpoint #{}] id={}", checkpointNumber, checkpointId.substring(0, 8));
        
        // 通知监听器触发 Checkpoint
        if (listener != null) {
            checkpointExecutor.submit(() -> {
                try {
                    listener.onCheckpointTriggered(checkpointNumber, checkpointId);
                } catch (Exception e) {
                    log.error("[kxj: Checkpoint 触发失败 #{}]", checkpointNumber, e);
                    checkpoint.recordOperatorFailure("coordinator", e);
                }
            });
        }
        
        // 设置超时检查
        scheduler.schedule(() -> checkTimeout(checkpointNumber), 
                          config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * 记录算子完成 Checkpoint
     */
    public void acknowledgeCheckpoint(long checkpointNumber, String operatorId, 
                                     com.kxj.streamingdataengine.state.Snapshot snapshot) {
        Checkpoint checkpoint = pendingCheckpoints.get(checkpointNumber);
        if (checkpoint == null) {
            log.warn("[kxj: Checkpoint #{} 不存在或已完成]", checkpointNumber);
            return;
        }
        
        checkpoint.recordOperatorSnapshot(operatorId, snapshot);
        
        if (checkpoint.getStatus() == Checkpoint.CheckpointStatus.COMPLETED) {
            completeCheckpoint(checkpointNumber);
        }
    }
    
    /**
     * 完成 Checkpoint
     */
    private void completeCheckpoint(long checkpointNumber) {
        Checkpoint checkpoint = pendingCheckpoints.remove(checkpointNumber);
        if (checkpoint == null) {
            return;
        }
        
        completedCheckpoints.put(checkpointNumber, checkpoint);
        
        // 清理旧的 Checkpoint
        cleanupOldCheckpoints();
        
        log.info("[kxj: Checkpoint #{} 完成] 耗时={}ms, 算子数={}",
                checkpointNumber, checkpoint.getDurationMs(), 
                checkpoint.getOperatorSnapshots().size());
        
        if (listener != null) {
            listener.onCheckpointCompleted(checkpointNumber, checkpoint);
        }
    }
    
    /**
     * 检查 Checkpoint 超时
     */
    private void checkTimeout(long checkpointNumber) {
        Checkpoint checkpoint = pendingCheckpoints.get(checkpointNumber);
        if (checkpoint == null || checkpoint.getStatus() != Checkpoint.CheckpointStatus.IN_PROGRESS) {
            return;
        }
        
        log.warn("[kxj: Checkpoint #{} 超时]", checkpointNumber);
        checkpoint.recordOperatorFailure("timeout", new TimeoutException("Checkpoint timeout"));
        pendingCheckpoints.remove(checkpointNumber);
        
        if (listener != null) {
            listener.onCheckpointFailed(checkpointNumber, new TimeoutException("Checkpoint timeout"));
        }
    }
    
    /**
     * 清理旧的 Checkpoint
     */
    private void cleanupOldCheckpoints() {
        // 保留最近 10 个成功的 Checkpoint
        long minToKeep = checkpointCounter.get() - 10;
        completedCheckpoints.entrySet().removeIf(e -> e.getKey() < minToKeep);
    }
    
    /**
     * 获取最新的成功 Checkpoint
     */
    public Checkpoint getLatestCompletedCheckpoint() {
        return completedCheckpoints.values().stream()
            .max((a, b) -> Long.compare(a.getCheckpointNumber(), b.getCheckpointNumber()))
            .orElse(null);
    }
    
    /**
     * 从最近的 Checkpoint 恢复
     */
    public boolean restoreFromLatestCheckpoint() {
        Checkpoint latest = getLatestCompletedCheckpoint();
        if (latest == null) {
            log.info("[kxj: 没有可用的 Checkpoint 用于恢复]");
            return false;
        }
        
        log.info("[kxj: 从 Checkpoint #{} 恢复]", latest.getCheckpointNumber());
        
        if (latest.getConsolidatedSnapshot() != null && stateBackend != null) {
            stateBackend.restoreFromSnapshot(latest.getConsolidatedSnapshot());
        }
        
        return true;
    }
    
    public interface CheckpointListener {
        void onCheckpointTriggered(long checkpointNumber, String checkpointId);
        void onCheckpointCompleted(long checkpointNumber, Checkpoint checkpoint);
        void onCheckpointFailed(long checkpointNumber, Throwable error);
    }
}
