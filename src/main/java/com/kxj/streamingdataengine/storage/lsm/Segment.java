package com.kxj.streamingdataengine.storage.lsm;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * 不可变磁盘Segment
 * 借鉴Kafka的Segment设计和ClickHouse的存储格式
 */
@Slf4j
class Segment<K extends Comparable<K>, V> implements Comparable<Segment<K, V>> {

    private static final long MAGIC_NUMBER = 0x4C534D5345474D45L; // "LSMSEGME"
    private static final int VERSION = 1;
    private static final int INDEX_INTERVAL = 128; // 每128条记录一个索引

    private final Path dataFile;
    @Getter
    private final Path indexFile;
    private final long minKey;
    private final long maxKey;
    private final long sequenceNumber;
    @Getter
    private final long entryCount;
    private final long size;

    // 内存索引（稀疏索引）
    private final NavigableMap<K, Long> sparseIndex;

    public Segment(Path dataFile, Path indexFile, long minKey, long maxKey,
                   long sequenceNumber, long entryCount, long size,
                   NavigableMap<K, Long> sparseIndex) {
        this.dataFile = dataFile;
        this.indexFile = indexFile;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.sequenceNumber = sequenceNumber;
        this.entryCount = entryCount;
        this.size = size;
        this.sparseIndex = sparseIndex;
    }

    /**
     * 从MemTable创建Segment
     */
    public static <K extends Comparable<K>, V> Segment<K, V> fromMemTable(MemTable<K, V> memTable) throws IOException {
        // 创建临时文件
        Path tempDir = Files.createTempDirectory("lsm_segment_");
        Path dataFile = tempDir.resolve("data");
        Path indexFile = tempDir.resolve("index");

        NavigableMap<K, Long> index = new TreeMap<>();
        List<EntryData> entries = new ArrayList<>();

        // 序列化数据
        long offset = 0;
        int count = 0;
        K minKey = null;
        K maxKey = null;

        for (Map.Entry<K, Entry<V>> entry : memTable.getAll().entrySet()) {
            K key = entry.getKey();
            Entry<V> value = entry.getValue();

            if (minKey == null) minKey = key;
            maxKey = key;

            // 构建稀疏索引
            if (count % INDEX_INTERVAL == 0) {
                index.put(key, offset);
            }

            // 序列化条目
            byte[] keyBytes = serializeKey(key);
            byte[] valueBytes = value.deleted() ? null : serializeValue(value.value());
            EntryData entryData = new EntryData(keyBytes, valueBytes, value.sequenceNumber(), value.deleted());
            entries.add(entryData);

            offset += entryData.getSerializedSize();
            count++;
        }

        // 写入数据文件
        try (FileChannel channel = FileChannel.open(dataFile,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.WRITE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {

            // 写入文件头
            ByteBuffer header = ByteBuffer.allocate(32);
            header.putLong(MAGIC_NUMBER);
            header.putInt(VERSION);
            header.putLong(minKey != null ? minKey.hashCode() : 0);
            header.putLong(maxKey != null ? maxKey.hashCode() : 0);
            header.putInt(count);
            header.flip();
            channel.write(header);

            // 写入条目
            for (EntryData entry : entries) {
                ByteBuffer buffer = entry.serialize();
                channel.write(buffer);
            }
        }

        // 写入索引文件
        writeIndex(indexFile, index);

        return new Segment<>(dataFile, indexFile,
                minKey != null ? minKey.hashCode() : 0,
                maxKey != null ? maxKey.hashCode() : 0,
                System.currentTimeMillis(), count, offset, index);
    }

    /**
     * 合并多个Segment
     */
    public static <K extends Comparable<K>, V> Segment<K, V> merge(List<Segment<K, V>> segments) throws IOException {
        // 合并所有数据，去除重复和已删除的
        TreeMap<K, Entry<V>> merged = new TreeMap<>();

        for (Segment<K, V> segment : segments) {
            // 读取所有数据
            List<Map.Entry<K, Entry<V>>> entries = segment.readAll();
            for (Map.Entry<K, Entry<V>> entry : entries) {
                K key = entry.getKey();
                Entry<V> value = entry.getValue();

                // 只保留最新的版本
                if (!merged.containsKey(key) ||
                    merged.get(key).sequenceNumber() < value.sequenceNumber()) {
                    merged.put(key, value);
                }
            }
        }

        // 创建新的MemTable并转换为Segment
        MemTable<K, V> memTable = new MemTable<>();
        long seq = 0;
        for (Map.Entry<K, Entry<V>> entry : merged.entrySet()) {
            if (entry.getValue().deleted()) {
                memTable.delete(entry.getKey(), seq++);
            } else {
                memTable.put(entry.getKey(), entry.getValue().value(), seq++);
            }
        }

        return fromMemTable(memTable);
    }

    /**
     * 查询数据
     */
    public Optional<Entry<V>> get(K key) {
        // 检查范围
        if (key.hashCode() < minKey || key.hashCode() > maxKey) {
            return Optional.empty();
        }

        // 使用稀疏索引定位
        Map.Entry<K, Long> floorEntry = sparseIndex.floorEntry(key);
        long startOffset = floorEntry != null ? floorEntry.getValue() : 32; // 跳过文件头

        try {
            return searchInFile(key, startOffset);
        } catch (IOException e) {
            log.error("Error reading segment", e);
            return Optional.empty();
        }
    }

    /**
     * 范围查询
     */
    public List<Map.Entry<K, Entry<V>>> range(K startKey, K endKey) {
        List<Map.Entry<K, Entry<V>>> result = new ArrayList<>();

        try {
            // 简化实现：读取所有过滤
            List<Map.Entry<K, Entry<V>>> all = readAll();
            for (Map.Entry<K, Entry<V>> entry : all) {
                if (entry.getKey().compareTo(startKey) >= 0 &&
                    entry.getKey().compareTo(endKey) <= 0) {
                    result.add(entry);
                }
            }
        } catch (IOException e) {
            log.error("Error reading segment", e);
        }

        return result;
    }

    /**
     * 读取所有数据
     */
    public List<Map.Entry<K, Entry<V>>> readAll() throws IOException {
        List<Map.Entry<K, Entry<V>>> result = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(dataFile, java.nio.file.StandardOpenOption.READ)) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            // 跳过文件头
            buffer.position(32);

            while (buffer.hasRemaining()) {
                EntryData entry = EntryData.deserialize(buffer);
                K key = deserializeKey(entry.keyBytes);
                V value = entry.isDeleted ? null : deserializeValue(entry.valueBytes);
                result.add(Map.entry(key, new Entry<>( value, entry.sequenceNumber, entry.isDeleted)));
            }
        }

        return result;
    }

    private Optional<Entry<V>> searchInFile(K key, long startOffset) throws IOException {
        try (FileChannel channel = FileChannel.open(dataFile, java.nio.file.StandardOpenOption.READ)) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            buffer.position((int) startOffset);

            while (buffer.hasRemaining()) {
                EntryData entry = EntryData.deserialize(buffer);
                K entryKey = deserializeKey(entry.keyBytes);

                int cmp = entryKey.compareTo(key);
                if (cmp == 0) {
                    V value = entry.isDeleted ? null : deserializeValue(entry.valueBytes);
                    return Optional.of(new Entry<>(value, entry.sequenceNumber, entry.isDeleted));
                } else if (cmp > 0) {
                    // 已经过了目标key
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    private static void writeIndex(Path indexFile, NavigableMap<?, Long> index) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(indexFile)))) {
            dos.writeInt(index.size());
            for (Map.Entry<?, Long> entry : index.entrySet()) {
                byte[] keyBytes = serializeKey(entry.getKey());
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(entry.getValue());
            }
        }
    }

    private static <K> byte[] serializeKey(K key) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(key);
            return baos.toByteArray();
        }
    }

    private static <V> byte[] serializeValue(V value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return baos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private static <K> K deserializeKey(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(bytes))) {
            try {
                return (K) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> V deserializeValue(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(bytes))) {
            try {
                return (V) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    public long size() {
        return size;
    }

    @Override
    public int compareTo(Segment<K, V> other) {
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    /**
         * 条目数据结构
         */
        private record EntryData(byte[] keyBytes, byte[] valueBytes, long sequenceNumber, boolean isDeleted) {

        int getSerializedSize() {
                return 4 + keyBytes.length + // key长度 + key
                        1 + // isDeleted标记
                        8 + // sequenceNumber
                        (isDeleted ? 0 : 4 + (valueBytes != null ? valueBytes.length : 0)); // value
            }

            ByteBuffer serialize() {
                ByteBuffer buffer = ByteBuffer.allocate(getSerializedSize());
                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);
                buffer.put((byte) (isDeleted ? 1 : 0));
                buffer.putLong(sequenceNumber);
                if (!isDeleted && valueBytes != null) {
                    buffer.putInt(valueBytes.length);
                    buffer.put(valueBytes);
                }
                buffer.flip();
                return buffer;
            }

            static EntryData deserialize(ByteBuffer buffer) {
                int keyLen = buffer.getInt();
                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                boolean isDeleted = buffer.get() != 0;
                long sequenceNumber = buffer.getLong();

                byte[] valueBytes = null;
                if (!isDeleted) {
                    int valueLen = buffer.getInt();
                    valueBytes = new byte[valueLen];
                    buffer.get(valueBytes);
                }

                return new EntryData(keyBytes, valueBytes, sequenceNumber, isDeleted);
            }
        }
}
