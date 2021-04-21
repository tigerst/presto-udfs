package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class GetIdentifierExpNum {

    private static final Long DEFAULT_NUM = 100L;

    private GetIdentifierExpNum(){

    }

    @SqlNullable
    @ScalarFunction("get_identifier_expnum")
    @Description("get identifier expnum")
    @SqlType(StandardTypes.BIGINT)
    public static Long getIdentifierExpNum(@SqlType(StandardTypes.VARCHAR) Slice identifierSlice) {

        if (identifierSlice == null) {
            return DEFAULT_NUM;
        }

        String identifier = identifierSlice.toStringUtf8();

        String md5 = signByMd5(identifier);
        if (md5 == null) {
            return DEFAULT_NUM;
        }

        return new BigInteger(new BigInteger(md5, 16).toString(10))
                .mod(new BigInteger("10")).longValue();
    }

    /**
     * 类似 python 的 int(hashlib.md5(identifier).hexdigest(), 16) % 10
     * @param str
     * @return
     */
    public static String signByMd5(String... str) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
        for (String temp : str) {
            md.update(temp.getBytes());
        }
        return String.format("%032x", new BigInteger(1, md.digest()));
    }
}
