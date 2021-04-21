package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseUrl {

    private static String lastUrlStr = null;
    private static URL url = null;
    private static Pattern p = null;
    private static String lastKey = null;

    private ParseUrl(){

    }

    @ScalarFunction("parse_url")
    @Description("返回 URL 中指定的部分。partToExtract 的有效值为：HOST, PATH, QUERY, REF,\n" +
            "PROTOCOL, AUTHORITY, FILE, and USERINFO.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseUrl(
            @SqlType(StandardTypes.VARCHAR) Slice urlSlice,
            @SqlType(StandardTypes.VARCHAR) Slice partToExtractSlice) {
        if (urlSlice == null || partToExtractSlice == null) {
            return null;
        }

        String url = urlSlice.toStringUtf8();
        String partToExtract = partToExtractSlice.toStringUtf8();

        String evaluate = evaluate(url, partToExtract);
        if (StringUtils.isNotBlank(evaluate)) {
            return Slices.utf8Slice(evaluate);
        }
        return null;
    }

    @ScalarFunction("parse_url")
    @Description("返回 URL 中指定的部分。partToExtract 的有效值为：HOST, PATH, QUERY, REF,\n" +
            "PROTOCOL, AUTHORITY, FILE, and USERINFO.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseUrl(
            @SqlType(StandardTypes.VARCHAR) Slice urlSlice,
            @SqlType(StandardTypes.VARCHAR) Slice partToExtractSlice,
            @SqlType(StandardTypes.VARCHAR) Slice keySlice) {
        if (urlSlice == null || partToExtractSlice == null || keySlice == null) {
            return null;
        }

        String url = urlSlice.toStringUtf8();
        String partToExtract = partToExtractSlice.toStringUtf8();
        String key = keySlice.toStringUtf8();

        String evaluate = evaluate(url, partToExtract, key);
        if (StringUtils.isNotBlank(evaluate)) {
            return Slices.utf8Slice(evaluate);
        }
        return null;
    }


    public static String evaluate(String urlStr, String partToExtract) {
        if (urlStr == null || partToExtract == null) {
            return null;
        }

        if (lastUrlStr == null || !urlStr.equals(lastUrlStr)) {
            try {
                url = new URL(urlStr);
            } catch (Exception e) {
                return null;
            }
        }
        lastUrlStr = urlStr;

        if (partToExtract.equals("HOST")) {
            return url.getHost();
        }
        if (partToExtract.equals("PATH")) {
            return url.getPath();
        }
        if (partToExtract.equals("QUERY")) {
            return url.getQuery();
        }
        if (partToExtract.equals("REF")) {
            return url.getRef();
        }
        if (partToExtract.equals("PROTOCOL")) {
            return url.getProtocol();
        }
        if (partToExtract.equals("FILE")) {
            return url.getFile();
        }
        if (partToExtract.equals("AUTHORITY")) {
            return url.getAuthority();
        }
        if (partToExtract.equals("USERINFO")) {
            return url.getUserInfo();
        }

        return null;
    }

    public static String evaluate(String urlStr, String partToExtract, String key) {
        if (!partToExtract.equals("QUERY")) {
            return null;
        }

        String query = evaluate(urlStr, partToExtract);
        if (query == null) {
            return null;
        }

        if (!key.equals(lastKey)) {
            p = Pattern.compile("(&|^)" + key + "=([^&]*)");
        }

        lastKey = key;
        Matcher m = p.matcher(query);
        if (m.find()) {
            return m.group(2);
        }
        return null;
    }
}
