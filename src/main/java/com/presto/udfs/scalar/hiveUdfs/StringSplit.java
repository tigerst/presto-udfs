package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StringSplit {

    private static final String COMMA_SEP = ",";
    private static final String VERTICAL_SEP = "\\|";
    private static final String NULL_STR = "NULL";

    @ScalarFunction("str_split")
    @Description("将str根据separator切分，每一项取从0到len长度的值，不足补NULL，并通过separator连接返回")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringSplit(@SqlType(StandardTypes.VARCHAR) Slice strSlice,
                          @SqlType(StandardTypes.VARCHAR) Slice separatorSlice,
                          @SqlType(StandardTypes.INTEGER) long len) {
        if (strSlice == null || separatorSlice == null) {
            return null;
        }

        String str = strSlice.toStringUtf8();
        String separator = separatorSlice.toStringUtf8();

        if (separator.equals("")) {
            return null;
        }

        String[] split = str.split(separator);
        if (split.length == 0 || len <= 0) {
            return null;
        }
        StringBuffer[] sbs = new StringBuffer[(int)len];
        for (int i = 0; i < len; i++) {
            sbs[i] = new StringBuffer();
        }

        for (int i = 0; i < split.length; i++) {
            String[] tmp = split[i].split(COMMA_SEP);

            for (int j = 0; j < len; j++) {
                String tmpStr = NULL_STR;
                if (j < tmp.length) {
                    if (StringUtils.isNotBlank(tmp[j])) {
                        if (tmp[j].toUpperCase().equals(NULL_STR)) {
                            tmpStr = NULL_STR;
                        } else {
                            tmpStr = tmp[j];
                        }
                    }
                }
                sbs[j].append(tmpStr).append(COMMA_SEP);
            }
        }

        List<String> list = Arrays.stream(sbs).map(sb -> sb.substring(0, sb.length() - 1)).collect(Collectors.toList());
        return Slices.utf8Slice(Joiner.on(separator).join(list));
    }

    @ScalarFunction("str_split")
    @Description("将str根据|切分，每一项根据,分割后第一项为0，第二项小于100，计数加1，返回总计数")
    @SqlType(StandardTypes.BIGINT)
    public static long stringSplit(@SqlType(StandardTypes.VARCHAR) Slice strSlice) {
        if (strSlice == null){
            return 0;
        }
        String str = strSlice.toStringUtf8();
        if (StringUtils.isBlank(str)) {
            return 0;
        }
        String[] split = str.split(VERTICAL_SEP);
        if (split.length == 0) {
            return 0;
        }

        long cnt = 0;
        for (int i = 0; i < split.length; i++) {
            String[] tmp = split[i].split(COMMA_SEP);
            if (tmp.length == 1 && Integer.valueOf(tmp[0].trim()) == 0){
                cnt++;
            } else if (tmp.length >= 2 && Integer.valueOf(tmp[0].trim()) == 0 && Integer.valueOf(tmp[1].trim()) < 100){
                cnt++;
            }
        }
        return cnt;
    }
}
