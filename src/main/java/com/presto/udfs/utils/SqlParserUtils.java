package com.presto.udfs.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.presto.udfs.enums.SqlTypeEnum;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlParserUtils {

    /**
     * druid sql解析
     * 不支持的语句：show / desc / describe / use db / load into table
     * @param sql
     * @return
     */
    public static SchemaStatVisitor parseHiveOrPresto(String sql) {
        sql = sql.trim().toLowerCase();
        if (sql.startsWith("show") || sql.startsWith("desc") || sql.startsWith("use")
                || sql.startsWith("describe") || sql.startsWith("load")) {
            return null;
        }
        HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
        List<SQLStatement> statements;
        try {
            statements = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        } catch (Exception e) {
            statements = SQLUtils.parseStatements(sql, JdbcConstants.PRESTO);
        }
        statements.forEach(st -> st.accept(visitor));
        return visitor;
    }

    /**
     * 获取sql中表的操作类型
     * @param sql
     * @return
     */
    public static Map<String, SqlTypeEnum> getTableOperInfo(String sql){
        SchemaStatVisitor visitor = null;
        try {
            visitor = parseHiveOrPresto(sql);
        } catch (Exception e) {
            return Maps.newHashMap();
        }
        if (null == visitor) {
            return Maps.newHashMap();
        }

        Map<TableStat.Name, TableStat> tables = visitor.getTables();
        Map<String, SqlTypeEnum> tableMap = tables.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> {
                    TableStat stat = entry.getValue();
                    if (stat.getInsertCount() > 0 && stat.getSelectCount() > 0) {
                        return SqlTypeEnum.INSERT_SELECT;
                    } else if (stat.getSelectCount() > 0) {
                        return SqlTypeEnum.SELECT;
                    } else if (stat.getInsertCount() > 0 || stat.getUpdateCount() > 0 || stat.getDeleteCount() > 0) {
                        return SqlTypeEnum.WRITE;
                    } else {
                        return SqlTypeEnum.DDL;
                    }
                }));
        return tableMap;
    }

    public static void main(String[] args) {

    }


}
