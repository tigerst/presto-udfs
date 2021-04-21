package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.base.Joiner;
import com.presto.udfs.enums.SqlTypeEnum;
import com.presto.udfs.utils.SqlParserUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ScalarFunction("parse_sql")
public class SqlFunction {

    @Description("Returns the tables divided by , of the sql string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseSql(@SqlType(StandardTypes.VARCHAR) Slice sqlSlice){
        String sql = sqlSlice.toStringUtf8().trim();
        Map<String, SqlTypeEnum> tableOperInfo = SqlParserUtils.getTableOperInfo(sql);
        List<String> list = tableOperInfo.entrySet().stream().map(entry -> {
            String table = entry.getKey();
            if (entry.getValue().equals(SqlTypeEnum.INSERT_SELECT)) {
                return String.format("%s|r,%s|w", table, table);
            } else if (entry.getValue().equals(SqlTypeEnum.SELECT)) {
                return String.format("%s|r", table);
            } else if (entry.getValue().equals(SqlTypeEnum.WRITE)) {
                return String.format("%s|w", table);
            } else {
                return String.format("%s|d", table);
            }
        }).collect(Collectors.toList());
        String tables = Joiner.on(",").join(list);
        return Slices.utf8Slice(tables);
    }
}
