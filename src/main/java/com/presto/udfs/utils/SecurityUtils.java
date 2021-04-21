package com.presto.udfs.utils;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class SecurityUtils {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    public static String md5(String str){
        return Hashing.md5().hashString(str, UTF_8).toString();
    }

    public static String sha512(String str){
        return Hashing.sha512().hashString(str, UTF_8).toString();
    }

    public static String b64encode(String str){
        return BaseEncoding.base64().encode(str.getBytes(UTF_8));
    }

    public static String b64decode(String str){
        return new String(BaseEncoding.base64().decode(str), UTF_8);
    }

    public static String b64encode(String str, Charset charset){
        return BaseEncoding.base64().encode(str.getBytes(charset));
    }

    public static String b64decode(String str, Charset charset){
        return new String(BaseEncoding.base64().decode(str), charset);
    }
}
