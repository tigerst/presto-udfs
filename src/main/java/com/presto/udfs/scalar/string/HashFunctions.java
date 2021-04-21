package com.presto.udfs.scalar.string;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.utils.SecurityUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.codec.digest.DigestUtils;

public class HashFunctions {

    private HashFunctions(){

    }

    @Description("md5 hash")
    @ScalarFunction("md5")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md5(@SqlType(StandardTypes.VARCHAR) Slice string) {
        return Slices.utf8Slice(DigestUtils.md5Hex(string.toStringUtf8()));
    }

    @Description("sha256 hash")
    @ScalarFunction("sha256")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha256(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }
        return Slices.utf8Slice(DigestUtils.sha256Hex(string.toStringUtf8()));
    }

    @Description("sha512 hash")
    @ScalarFunction("sha512")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha512(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }
        return Slices.utf8Slice(DigestUtils.sha512Hex(string.toStringUtf8()));
    }

    @Description("混合加密，加密方式为，先sha512加密，然后base64加密，最后md5加密")
    @ScalarFunction("hybird_encode")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice hybirdEncode(@SqlType(StandardTypes.VARCHAR) Slice string){
        String base64Code = SecurityUtils.b64encode(SecurityUtils.sha512(string.toStringUtf8()))
                .replace("\r", "")
                .replace("\n", "");
        String reverseCode = new StringBuffer(base64Code).reverse().toString();
        return Slices.utf8Slice(SecurityUtils.md5(reverseCode));
    }

}
