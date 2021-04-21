package com.presto.udfs.geoip;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BytesUtils {

    /**
     * 字节数组转换成无符号int
     * 大端模式 （默认）
     *
     * @param bytes
     * @return
     */
    public static int bytesToUnsignedInt(byte[] bytes){
        return bytesToUnsignedIntBE(bytes);
    }

    /**
     * 字节数组转换成无符号int
     * 大端模式
     *
     * @param bytes
     * @return
     */
    public static int bytesToUnsignedIntBE(byte[] bytes){
        return bytesToUnsignedInt(bytes, ByteOrder.BIG_ENDIAN);
    }

    /**
     * 字节数组转换成无符号int
     * 小端模式
     *
     * @param bytes
     * @return
     */
    public static int bytesToUnsignedIntLE(byte[] bytes){
        return bytesToUnsignedInt(bytes, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * 字节数组转换成无符号int
     *
     * @param bytes
     * @return
     */
    public static int bytesToUnsignedInt(byte[] bytes, ByteOrder order){
        return Integer.parseUnsignedInt(String.valueOf(bytesToInt(bytes, order)));
    }

    /**
     * 字节数组转换成int
     * 大端模式 （默认）
     *
     * @param bytes
     * @return
     */
    public static int bytesToInt(byte[] bytes){
        return bytesToIntBE(bytes);
    }

    /**
     * 字节数组转换成int
     * 大端
     *
     * @param bytes
     * @return
     */
    public static int bytesToIntBE(byte[] bytes){
        return bytesToInt(bytes, ByteOrder.BIG_ENDIAN);
    }

    /**
     * 字节数组转换成int
     * 小端
     *
     * @param bytes
     * @return
     */
    public static int bytesToIntLE(byte[] bytes){
        return bytesToInt(bytes, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * 字节数组转换成int
     *
     * @param bytes
     * @param order
     * @return
     */
    public static int bytesToInt(byte[] bytes, ByteOrder order){
        return ByteBuffer.wrap(bytes).order(order).getInt();
    }

    /**
     * byte数组转换成无符号长整形
     * 大端模式
     * @param bytes
     * @return
     */
    public static long pack(byte[] bytes) {
        long val = 0;
        for (int i = 0; i < bytes.length; i++) {
            val <<= 8;
            val |= bytes[i] & 0xff;
        }
        return val;
    }

    /**
     * 无符号长整型转换成byte数组
     * 大端模式
     * @param bytes
     * @return
     */
    public static byte[] unpack(long bytes) {
        return new byte[] {
                (byte)((bytes >>> 56) & 0xff),
                (byte)((bytes >>> 48) & 0xff),
                (byte)((bytes >>> 40) & 0xff),
                (byte)((bytes >>> 32) & 0xff),
                (byte)((bytes >>> 24) & 0xff),
                (byte)((bytes >>> 16) & 0xff),
                (byte)((bytes >>>  8) & 0xff),
                (byte)((bytes       ) & 0xff)
        };
    }

    public static void main(String[] args) {

    }

}
