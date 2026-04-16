package com.kxj.streamingdataengine.storage.lsm;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

/**
 * Write-Ahead Log (WAL)
 * 保证数据可靠性
 */
@Slf4j
public class WriteAheadLog implements Closeable {

    private static final long MAGIC = 0x57414C4C4F47L; // "WALLOG"
    private static final int RECORD_HEADER_SIZE = 20; // magic(8) + size(4) + crc(8)

    private final Path logFile;
    private final FileChannel channel;
    private final BlockingQueue<LogEntry> writeQueue;
    private final AtomicBoolean running;
    private final Thread flushThread;

    public WriteAheadLog(Path logDir) throws IOException {
        this.logFile = logDir.resolve("wal.log");
        Files.createDirectories(logDir);

        this.channel = FileChannel.open(logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.SYNC); // 同步写

        this.writeQueue = new LinkedBlockingQueue<>();
        this.running = new AtomicBoolean(true);

        // 启动刷盘线程
        this.flushThread = Thread.ofVirtual().start(this::flushLoop);
    }

    /**
     * 追加写入
     */
    public void append(Object key, Object value) {
        LogEntry entry = new LogEntry(LogEntry.Type.PUT, key, value);
        writeQueue.offer(entry);
    }

    /**
     * 追加删除
     */
    public void appendDelete(Object key) {
        LogEntry entry = new LogEntry(LogEntry.Type.DELETE, key, null);
        writeQueue.offer(entry);
    }

    /**
     * 刷盘循环
     */
    private void flushLoop() {
        while (running.get()) {
            try {
                LogEntry entry = writeQueue.take();
                writeEntry(entry);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                log.error("WAL write failed", e);
            }
        }

        // 刷剩余数据
        while (!writeQueue.isEmpty()) {
            try {
                writeEntry(writeQueue.poll());
            } catch (IOException e) {
                log.error("WAL final flush failed", e);
            }
        }
    }

    private void writeEntry(LogEntry entry) throws IOException {
        byte[] keyBytes = ObjectSerializer.serialize(entry.key);
        byte[] valueBytes = entry.value != null ? ObjectSerializer.serialize(entry.value) : null;

        int valueSize = valueBytes != null ? valueBytes.length : 0;
        int totalSize = 1 + 4 + keyBytes.length + 4 + valueSize; // type + keylen + key + valuelen + value

        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE + totalSize);
        buffer.putLong(MAGIC);
        buffer.putInt(totalSize);

        // CRC校验
        CRC32 crc = new CRC32();

        // 写入数据
        buffer.put((byte) entry.type.ordinal());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueSize);
        if (valueBytes != null) {
            buffer.put(valueBytes);
        }

        // 计算CRC
        buffer.position(RECORD_HEADER_SIZE);
        byte[] data = new byte[totalSize];
        buffer.get(data);
        crc.update(data);

        buffer.putLong(12, crc.getValue());
        buffer.position(0);

        channel.write(buffer);
    }

    /**
     * 恢复数据
     */
    public void recover(WALHandler handler) throws IOException {
        if (!Files.exists(logFile) || Files.size(logFile) == 0) {
            return;
        }

        try (FileChannel readChannel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            long position = 0;

            while (position < readChannel.size()) {
                readChannel.position(position);
                buffer.clear();

                // 读取header
                int read = readChannel.read(buffer);
                if (read < RECORD_HEADER_SIZE) {
                    break;
                }

                buffer.flip();
                long magic = buffer.getLong();
                if (magic != MAGIC) {
                    log.warn("Invalid WAL record at position {}", position);
                    break;
                }

                int size = buffer.getInt();
                long storedCrc = buffer.getLong();

                // 读取数据
                ByteBuffer dataBuffer = ByteBuffer.allocate(size);
                int dataRead = readChannel.read(dataBuffer);
                if (dataRead < size) {
                    log.warn("Incomplete WAL record at position {}", position);
                    break;
                }

                dataBuffer.flip();

                // 验证CRC
                CRC32 crc = new CRC32();
                byte[] data = new byte[size];
                dataBuffer.get(data);
                crc.update(data);

                if (crc.getValue() != storedCrc) {
                    log.warn("CRC mismatch at position {}", position);
                    break;
                }

                // 解析条目
                dataBuffer.position(0);
                LogEntry.Type type = LogEntry.Type.values()[dataBuffer.get()];
                int keyLen = dataBuffer.getInt();
                byte[] keyBytes = new byte[keyLen];
                dataBuffer.get(keyBytes);
                Object key = ObjectSerializer.deserialize(keyBytes);

                if (type == LogEntry.Type.PUT) {
                    int valueLen = dataBuffer.getInt();
                    byte[] valueBytes = new byte[valueLen];
                    dataBuffer.get(valueBytes);
                    Object value = ObjectSerializer.deserialize(valueBytes);
                    handler.onPut(key, value);
                } else {
                    handler.onDelete(key);
                }

                position += RECORD_HEADER_SIZE + size;
            }
        }
    }

    /**
     * 清空WAL
     */
    public void clear() throws IOException {
        channel.truncate(0);
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        flushThread.interrupt();
        try {
            flushThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        channel.close();
    }

    /**
     * WAL恢复处理器
     */
    public interface WALHandler {
        void onPut(Object key, Object value);
        void onDelete(Object key);
    }

    /**
     * 日志条目
     */
    private record LogEntry(Type type, Object key, Object value) {
        enum Type {
            PUT, DELETE
        }
    }
}
