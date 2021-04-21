package com.presto.udfs.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtils {

	/**
	 * 默认编码
	 */
	private static final Charset encoding = StandardCharsets.ISO_8859_1;

	/**
	 * GZIP 压缩字节数组
	 * @param bytes
	 * @return
	 */
	public static byte[] compressMsg(byte[] bytes) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gzip;
		try {
			gzip = new GZIPOutputStream(bos);
			gzip.write(bytes);
			gzip.finish();
			gzip.close();
			byte[] result = bos.toByteArray();
			bos.close();
			return result;
		} catch (IOException e) {
			return bytes;
		} finally {
			bos = null;
			gzip = null;
		}
	}

	/**
	 * GZIP 解压缩字节数组
	 * @param bytes
	 * @return
	 */
	public static byte[] uncompressMsg(byte[] bytes) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPInputStream gzip;
		try {
			gzip = new GZIPInputStream(new ByteArrayInputStream(bytes));
			byte[] buff = new byte[1024];
			int len;
			while ((len = gzip.read(buff)) != -1) {
				bos.write(buff, 0, len);
			}
			gzip.close();
			byte[] result = bos.toByteArray();
			bos.close();
			return result;
		} catch (IOException e) {
			return bytes;
		} finally {
			bos = null;
			gzip = null;
		}
	}

	/**
	 * GZIP 压缩
	 * @param targetStr
	 * @return
	 */
	public static String compressMsg(String targetStr) {
		return new String(compressMsg(targetStr.getBytes(encoding)), encoding);
	}

	/**
	 * GZIP 解压缩
	 * @param compressedStr
	 * @return
	 */
	public static String uncompressMsg(String compressedStr) {
		return new String(uncompressMsg(compressedStr.getBytes(encoding)), encoding);
	}

	/**
	 * base64字符串压缩，处理顺序，compress --> base64 encode
	 * @param targetStr
	 * @return
	 */
	public static String compressB64Msg(String targetStr) {
		return SecurityUtils.b64encode(compressMsg(targetStr), encoding);
	}

	/**
	 * base64字符串解压缩，处理顺序，base64 decode --> uncompress
	 * @param compressedStr
	 * @return
	 */
	public static String uncompressB64Msg(String compressedStr){
		return uncompressMsg(SecurityUtils.b64decode(compressedStr, encoding));
	}

	/**
	 * base64字符串压缩，处理顺序，compress --> base64 encode
	 * @param targetStr
	 * @param encoding	指定编码
	 * @return
	 */
	public static String compressB64Msg(String targetStr, Charset encoding) {
		return SecurityUtils.b64encode(compressMsg(targetStr), encoding);
	}

	/**
	 * base64字符串解压缩，处理顺序，base64 decode --> uncompress
	 * @param compressedStr
	 * @param encoding	指定编码
	 * @return
	 */
	public static String uncompressB64Msg(String compressedStr, Charset encoding){
		return uncompressMsg(SecurityUtils.b64decode(compressedStr, encoding));
	}

	public static void main(String[] args) {
		String msg = "H4sIAAAAAAAAAJXRrQ6DQBAE4HdZfYL738VWNamsI4QcxwoEpSlUNbx7y8kGxLoZMcmXTPOBPE/PeeHrADV4jY565gox+MQpU6qod5mzocBm6LTtNCjgx3AfJ4ZaB+0RbXAYdVQwpXyZ348V6qqU27j8ctMqWNb0Wv821nrclFTgjgXeYrBSwa6WC/yJgExEoaCo5YJwLIgukpEKdrVcEE8ERFYLBUUtF+CxgLz8haLe2i+Hd5muDQMAAA==";
		System.out.println(uncompressB64Msg(msg));
	}

}
