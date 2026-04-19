package com.kxj.streamingdataengine.checkpoint;

import com.kxj.streamingdataengine.core.model.StreamRecord;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint Barrier 对齐机制测试
 * 
 * 验证核心功能：
 * 1. 单输入场景：Barrier 立即触发快照（无需对齐）
 * 2. 多输入场景：必须等待所有输入都收到 Barrier
 * 3. 数据缓存：Barrier 之后的数据应该被缓存
 * 4. 对齐完成：触发快照并释放缓存数据
 */
@DisplayName("Checkpoint Barrier 对齐机制测试")
class AligningBarrierHandlerTest {

    private List<InputChannel> channels;
    private AtomicLong triggeredCheckpointId;
    private Map<InputChannel, List<StreamRecord<?>>> capturedBufferedRecords;
    private AligningBarrierHandler handler;
    
    @BeforeEach
    void setUp() {
        // 创建两个输入通道（模拟多输入算子，如 union）
        channels = List.of(
            new InputChannel(1, "input-1"),
            new InputChannel(2, "input-2")
        );
        
        triggeredCheckpointId = new AtomicLong(-1);
        capturedBufferedRecords = new ConcurrentHashMap<>();
        
        // 创建 Handler
        handler = new AligningBarrierHandler(channels, (checkpointId, bufferedRecords) -> {
            triggeredCheckpointId.set(checkpointId);
            capturedBufferedRecords.putAll(bufferedRecords);
        });
    }
    
    @Test
    @DisplayName("测试单输入场景 - 只有一个通道")
    void testSingleInput() {
        List<InputChannel> singleChannel = List.of(new InputChannel("single"));
        AtomicLong checkpointId = new AtomicLong(-1);
        
        AligningBarrierHandler singleHandler = new AligningBarrierHandler(
            singleChannel, 
            (id, records) -> checkpointId.set(id)
        );
        
        CheckpointBarrier barrier = new CheckpointBarrier(1);
        AligningBarrierHandler.BarrierProcessResult result = 
            singleHandler.onBarrier(barrier, singleChannel.get(0));
        
        // 单输入应该立即对齐（无需等待）
        assertTrue(result.isAligned(), "单输入应该立即对齐");
        assertEquals(1, checkpointId.get(), "应该触发 checkpoint 1");
    }
    
    @Test
    @DisplayName("测试多输入对齐 - 必须等待所有通道")
    void testMultiInputAlignment() {
        InputChannel channel1 = channels.get(0);
        InputChannel channel2 = channels.get(1);
        
        CheckpointBarrier barrier = new CheckpointBarrier(1);
        
        // 第一个通道收到 Barrier
        AligningBarrierHandler.BarrierProcessResult result1 = 
            handler.onBarrier(barrier, channel1);
        
        assertTrue(result1.isBuffer(), "第一个通道应该进入等待状态");
        assertEquals(-1, triggeredCheckpointId.get(), "不应该立即触发快照");
        
        // 第二个通道收到 Barrier - 对齐完成
        AligningBarrierHandler.BarrierProcessResult result2 = 
            handler.onBarrier(barrier, channel2);
        
        assertTrue(result2.isAligned(), "第二个通道应该触发对齐完成");
        assertEquals(1, triggeredCheckpointId.get(), "应该触发 checkpoint 1");
    }
    
    @Test
    @DisplayName("测试数据缓存 - Barrier 之后的数据应该被缓存")
    void testDataBuffering() {
        InputChannel channel1 = channels.get(0);
        InputChannel channel2 = channels.get(1);
        
        // 创建测试数据
        StreamRecord<String> record1 = createRecord("data-1");
        StreamRecord<String> record2 = createRecord("data-2");
        
        // channel1 收到 Barrier
        handler.onBarrier(new CheckpointBarrier(1), channel1);
        
        // channel1 后续数据应该被缓存
        boolean proceed1 = handler.onRecord(record1, channel1);
        assertFalse(proceed1, "Barrier 后的数据应该被缓存，不继续处理");
        
        // channel2 还没收到 Barrier，数据应该正常处理
        boolean proceed2 = handler.onRecord(record2, channel2);
        assertTrue(proceed2, "未收到 Barrier 的通道数据应该正常处理");
        
        // channel2 收到 Barrier，触发对齐
        handler.onBarrier(new CheckpointBarrier(1), channel2);
        
        // 验证缓存的数据
        assertEquals(1, capturedBufferedRecords.get(channel1).size(), 
                    "channel1 应该有 1 条缓存数据");
        assertTrue(capturedBufferedRecords.get(channel2) == null || capturedBufferedRecords.get(channel2).isEmpty(), 
                  "channel2 应该没有缓存数据");
    }
    
    @Test
    @DisplayName("测试多 Checkpoint - 新 Checkpoint 打断旧对齐")
    void testNewCheckpointInterruption() {
        InputChannel channel1 = channels.get(0);
        InputChannel channel2 = channels.get(1);
        
        // Checkpoint 1: channel1 收到 Barrier
        handler.onBarrier(new CheckpointBarrier(1), channel1);
        assertEquals(-1, triggeredCheckpointId.get());
        
        // Checkpoint 2: channel2 收到 Barrier（打断 Checkpoint 1）
        AligningBarrierHandler.BarrierProcessResult result = 
            handler.onBarrier(new CheckpointBarrier(2), channel2);
        
        // 新 Checkpoint 打断旧对齐，channel2 需要等待 channel1
        assertTrue(result.isBuffer(), "应该进入等待状态");
        assertEquals(-1, triggeredCheckpointId.get(), "Checkpoint 1 应该被取消");
        
        // channel1 收到 Checkpoint 2 的 Barrier
        result = handler.onBarrier(new CheckpointBarrier(2), channel1);
        
        // 对齐完成
        assertTrue(result.isAligned(), "应该对齐完成");
        assertEquals(2, triggeredCheckpointId.get(), "应该触发 checkpoint 2");
    }
    
    @Test
    @DisplayName("测试对齐状态监控")
    void testAlignmentStatus() {
        InputChannel channel1 = channels.get(0);
        InputChannel channel2 = channels.get(1);
        
        // 初始状态
        AligningBarrierHandler.AlignmentStatus initialStatus = handler.getStatus();
        assertFalse(initialStatus.isAligning());
        assertEquals(0, initialStatus.getProgress(), 0.01);
        
        // channel1 收到 Barrier
        handler.onBarrier(new CheckpointBarrier(1), channel1);
        
        AligningBarrierHandler.AlignmentStatus midStatus = handler.getStatus();
        assertTrue(midStatus.isAligning());
        assertEquals(0.5, midStatus.getProgress(), 0.01); // 1/2 = 0.5
        assertEquals(1, midStatus.receivedBarriers());
        
        // channel2 收到 Barrier
        handler.onBarrier(new CheckpointBarrier(1), channel2);
        
        AligningBarrierHandler.AlignmentStatus finalStatus = handler.getStatus();
        assertFalse(finalStatus.isAligning()); // 对齐完成
        assertEquals(0, finalStatus.getProgress(), 0.01);
    }
    
    @Test
    @DisplayName("测试并发场景 - 多线程发送 Barrier")
    void testConcurrentBarriers() throws InterruptedException {
        int numThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicLong successCount = new AtomicLong(0);
        
        // 创建多通道 Handler
        List<InputChannel> multiChannels = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            multiChannels.add(new InputChannel(i, "channel-" + i));
        }
        
        AligningBarrierHandler multiHandler = new AligningBarrierHandler(
            multiChannels, 
            (id, records) -> successCount.incrementAndGet()
        );
        
        // 只测试一个 Checkpoint 的并发对齐
        long checkpointId = 1;
        
        // 多线程并发发送同一个 Checkpoint 的 Barrier
        for (int i = 0; i < numThreads; i++) {
            final int channelIdx = i;
            executor.submit(() -> {
                try {
                    multiHandler.onBarrier(
                        new CheckpointBarrier(checkpointId), 
                        multiChannels.get(channelIdx)
                    );
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        // 验证：Checkpoint 应该成功对齐一次
        assertEquals(1, successCount.get(), 
                    "Checkpoint 应该成功对齐一次");
    }
    
    @Test
    @DisplayName("测试不同 Checkpoint ID 混合")
    void testMixedCheckpointIds() {
        InputChannel channel1 = channels.get(0);
        InputChannel channel2 = channels.get(1);
        
        // channel1 收到 Checkpoint 1
        handler.onBarrier(new CheckpointBarrier(1), channel1);
        
        // channel2 收到 Checkpoint 2（乱序，应该打断 Checkpoint 1）
        AligningBarrierHandler.BarrierProcessResult result = 
            handler.onBarrier(new CheckpointBarrier(2), channel2);
        
        assertTrue(result.isBuffer());
        assertEquals(-1, triggeredCheckpointId.get());
        
        // channel1 收到 Checkpoint 2
        result = handler.onBarrier(new CheckpointBarrier(2), channel1);
        
        assertTrue(result.isAligned());
        assertEquals(2, triggeredCheckpointId.get());
    }
    
    // ===== 辅助方法 =====
    
    private StreamRecord<String> createRecord(String value) {
        return StreamRecord.<String>builder()
            .key(UUID.randomUUID().toString())
            .value(value)
            .eventTime(System.currentTimeMillis())
            .partition(0)
            .sequenceNumber(1)
            .build();
    }
}
