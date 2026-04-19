package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import com.kxj.streamingdataengine.execution.ExecutionEngine;
import com.kxj.streamingdataengine.state.StateBackend;
import com.kxj.streamingdataengine.state.StateBackendFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint 机制测试
 */
@DisplayName("Checkpoint 机制测试")
class CheckpointTest {

    @TempDir
    Path tempDir;

    private StateBackend stateBackend;
    private ExecutionEngine executionEngine;

    @BeforeEach
    void setUp() {
        stateBackend = StateBackendFactory.createMemoryBackend();
    }

    @AfterEach
    void tearDown() {
        if (executionEngine != null) {
            executionEngine.stop();
        }
        if (stateBackend != null) {
            stateBackend.close();
        }
    }

    @Test
    @DisplayName("测试 CheckpointBarrier 创建")
    void testCheckpointBarrierCreation() {
        CheckpointBarrier barrier = new CheckpointBarrier(1);

        assertNotNull(barrier.getCheckpointId());
        assertEquals(1, barrier.getCheckpointNumber());
        assertNotNull(barrier.getTimestamp());
        assertTrue(barrier.getBarrierTimestamp() > 0);
    }

    @Test
    @DisplayName("测试 CheckpointBarrier 对齐")
    void testCheckpointBarrierAlignment() {
        CheckpointBarrier barrier1 = new CheckpointBarrier(1, 1000);
        CheckpointBarrier barrier2 = new CheckpointBarrier(2, 2000);

        CheckpointBarrier aligned = barrier1.alignWith(barrier2);

        assertEquals(2, aligned.getCheckpointNumber()); // 取最大值
        assertEquals(2000, aligned.getBarrierTimestamp()); // 取最大值
        assertEquals(1, aligned.getPriority()); // 优先级增加
    }

    @Test
    @DisplayName("测试 CheckpointConfig 默认配置")
    void testCheckpointConfigDefaults() {
        CheckpointConfig config = CheckpointConfig.defaultConfig();

        assertTrue(config.isEnabled());
        assertEquals(Duration.ofMinutes(1), config.getInterval());
        assertEquals(Duration.ofMinutes(10), config.getTimeout());
        assertEquals(1, config.getMaxConcurrentCheckpoints());
        assertTrue(config.isIncrementalCheckpointsEnabled());
        assertFalse(config.isUnalignedCheckpointsEnabled());
    }

    @Test
    @DisplayName("测试 CheckpointCoordinator 启动和停止")
    void testCheckpointCoordinatorLifecycle() {
        CheckpointConfig config = CheckpointConfig.builder()
            .enabled(true)
            .interval(Duration.ofMillis(100))
            .maxConcurrentCheckpoints(5)  // 增加并发数，避免第一个未确认时阻止后续触发
            .build();

        AtomicInteger triggerCount = new AtomicInteger(0);
        CheckpointCoordinator[] coordinatorRef = new CheckpointCoordinator[1];
        
        CheckpointCoordinator coordinator = new CheckpointCoordinator(
            config, 
            stateBackend,
            new CheckpointCoordinator.CheckpointListener() {
                @Override
                public void onCheckpointTriggered(long checkpointNumber, String checkpointId) {
                    triggerCount.incrementAndGet();
                    // 立即确认 checkpoint，避免阻塞后续触发
                    coordinatorRef[0].acknowledgeCheckpoint(checkpointNumber, "test-operator", stateBackend.createSnapshot());
                }
                @Override
                public void onCheckpointCompleted(long checkpointNumber, Checkpoint checkpoint) {}
                @Override
                public void onCheckpointFailed(long checkpointNumber, Throwable error) {}
            }
        );
        coordinatorRef[0] = coordinator;

        coordinator.start();

        // 等待至少 2 次 Checkpoint 触发
        assertTimeout(Duration.ofSeconds(2), () -> {
            while (triggerCount.get() < 2) {
                Thread.sleep(50);
            }
        });

        coordinator.stop();
        int finalCount = triggerCount.get();

        // 停止后不应再触发新的 Checkpoint
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertEquals(finalCount, triggerCount.get());
    }

    @Test
    @DisplayName("测试 Checkpoint 完成流程")
    void testCheckpointCompletion() throws InterruptedException, ExecutionException, TimeoutException {
        CheckpointConfig config = CheckpointConfig.builder()
            .enabled(true)
            .interval(Duration.ofMillis(50))
            .build();

        AtomicReference<Checkpoint> completedCheckpoint = new AtomicReference<>();
        CompletableFuture<Long> completionFuture = new CompletableFuture<>();
        AtomicReference<CheckpointCoordinator> coordinatorRef = new AtomicReference<>();

        CheckpointCoordinator coordinator = new CheckpointCoordinator(
            config,
            stateBackend,
            new CheckpointCoordinator.CheckpointListener() {
                @Override
                public void onCheckpointTriggered(long checkpointNumber, String checkpointId) {
                    // 模拟算子完成快照
                    CheckpointCoordinator coord = coordinatorRef.get();
                    if (coord != null) {
                        coord.acknowledgeCheckpoint(
                            checkpointNumber,
                            "operator-1",
                            stateBackend.createSnapshot()
                        );
                    }
                }
                @Override
                public void onCheckpointCompleted(long checkpointNumber, Checkpoint checkpoint) {
                    completedCheckpoint.set(checkpoint);
                    completionFuture.complete(checkpointNumber);
                }
                @Override
                public void onCheckpointFailed(long checkpointNumber, Throwable error) {}
            }
        );
        coordinatorRef.set(coordinator);

        coordinator.start();

        // 等待 Checkpoint 完成
        Long checkpointNumber = completionFuture.get(3, TimeUnit.SECONDS);
        assertNotNull(checkpointNumber);
        assertNotNull(completedCheckpoint.get());
        assertEquals(Checkpoint.CheckpointStatus.COMPLETED, 
                    completedCheckpoint.get().getStatus());

        coordinator.stop();
    }

    @Test
    @DisplayName("测试 Checkpoint 超时")
    void testCheckpointTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        CheckpointConfig config = CheckpointConfig.builder()
            .enabled(true)
            .interval(Duration.ofHours(1)) // 不自动触发
            .timeout(Duration.ofMillis(100)) // 很短的超时
            .build();

        AtomicReference<Throwable> failureError = new AtomicReference<>();
        CompletableFuture<Long> failureFuture = new CompletableFuture<>();

        CheckpointCoordinator coordinator = new CheckpointCoordinator(
            config,
            stateBackend,
            new CheckpointCoordinator.CheckpointListener() {
                @Override
                public void onCheckpointTriggered(long checkpointNumber, String checkpointId) {
                    // 不确认 Checkpoint，让它超时
                }
                @Override
                public void onCheckpointCompleted(long checkpointNumber, Checkpoint checkpoint) {}
                @Override
                public void onCheckpointFailed(long checkpointNumber, Throwable error) {
                    failureError.set(error);
                    failureFuture.complete(checkpointNumber);
                }
            }
        );

        coordinator.start();
        coordinator.triggerCheckpoint(); // 手动触发

        // 等待超时
        Long checkpointNumber = failureFuture.get(3, TimeUnit.SECONDS);
        assertNotNull(checkpointNumber);
        assertNotNull(failureError.get());
        assertInstanceOf(java.util.concurrent.TimeoutException.class, failureError.get());

        coordinator.stop();
    }

    @Test
    @DisplayName("测试 ExecutionEngine 集成 Checkpoint")
    void testExecutionEngineCheckpointIntegration() {
        // 创建新的 ExecutionEngine 用于此测试
        ExecutionEngine engine = new ExecutionEngine(
            2,
            Duration.ofMillis(100),
            false,
            false
        );

        CheckpointConfig config = CheckpointConfig.builder()
            .enabled(true)
            .interval(Duration.ofMillis(100))
            .build();

        AtomicInteger triggerCount = new AtomicInteger(0);

        engine.initializeCheckpoint(
            config,
            stateBackend,
            new CheckpointCoordinator.CheckpointListener() {
                @Override
                public void onCheckpointTriggered(long checkpointNumber, String checkpointId) {
                    triggerCount.incrementAndGet();
                }
                @Override
                public void onCheckpointCompleted(long checkpointNumber, Checkpoint checkpoint) {}
                @Override
                public void onCheckpointFailed(long checkpointNumber, Throwable error) {}
            }
        );

        engine.start();

        // 验证 CheckpointCoordinator 已初始化
        assertNotNull(engine.getCheckpointCoordinator());

        // 等待至少一次 Checkpoint 触发
        assertTimeout(Duration.ofSeconds(2), () -> {
            while (triggerCount.get() < 1) {
                Thread.sleep(50);
            }
        });

        assertTrue(triggerCount.get() >= 1);
        
        engine.stop();
    }

    @Test
    @DisplayName("测试 StreamRecord 携带 CheckpointBarrier")
    void testStreamRecordWithCheckpointBarrier() {
        CheckpointBarrier barrier = new CheckpointBarrier(42);

        StreamRecord<CheckpointBarrier> record = StreamRecord.<CheckpointBarrier>builder()
            .key("checkpoint-key")
            .value(barrier)
            .eventTime(System.currentTimeMillis())
            .partition(0)
            .sequenceNumber(1)
            .build();

        assertNotNull(record);
        assertEquals(42, record.value().getCheckpointNumber());
        assertInstanceOf(StreamRecord.SpecialRecord.class, record.value());
    }
}
