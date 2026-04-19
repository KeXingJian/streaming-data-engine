package com.kxj.streamingdataengine.storage.lsm;

import java.io.*;

/**
 * 对象序列化工具
 * 提供泛型的序列化和反序列化方法
 */
class ObjectSerializer {

    private ObjectSerializer() {
        // 工具类，禁止实例化
    }

    /**
     * 将对象序列化为字节数组
     */
    static byte[] serialize(Object obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }

    /**
     * 将字节数组反序列化为对象
     */
    @SuppressWarnings("unchecked")
    static <T> T deserialize(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(bytes))) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }
}
