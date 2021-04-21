package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.presto.udfs.utils.JsonUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class ParseJsonKeys {

    private ParseJsonKeys(){

    }

    @ScalarFunction(value = "parse_json_keys")
    @Description("获取所有的key")
    @SqlType("array(varchar)")
    public static Block parseJsonKeys(@SqlType(StandardTypes.VARCHAR) Slice jsonSlice) {
        if (jsonSlice == null) {
            return null;
        }
        String json = jsonSlice.toStringUtf8();
        if (!JsonUtil.isJSONString(json)) {
            return null;
        }
        List<String> res = Lists.newArrayList();
        JsonUtil.getAllJsonKeys(json, res);
//        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), res.size());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, res.size());
        for (String str : res) {
            VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(str));
        }
        return blockBuilder;
    }

    @ScalarFunction(value = "parse_json_keys")
    @Description("获取第step层所有的key")
    @SqlType("array(varchar)")
    public static Block parseJsonKeysStep(@SqlType(StandardTypes.VARCHAR) Slice jsonSlice, @SqlType(StandardTypes.INTEGER) long step) {
        if (jsonSlice == null) {
            return null;
        }

        String json = jsonSlice.toStringUtf8();
        if (!JsonUtil.isJSONString(json)) {
            return null;
        }

        List<String> res = Lists.newArrayList();

        Queue<JsonNode> queue = new LinkedList<>();
        queue.offer(JsonUtil.getJsonNode(json));
        while (queue.size() > 0 && step > 0) {
            int len = queue.size();
            for (int i = 0; i < len; i++) {
                JsonNode peek = queue.peek();
                queue.poll();
                if (step == 1) {
                    JsonUtil.getJsonKeys(peek, res);
                }

                if (step > 1) {
                    Iterator<JsonNode> elements = peek.elements();
                    while (elements.hasNext()) {
                        queue.offer(elements.next());
                    }
                }
            }
            step--; // 降为1时，开始取key值
        }

        if (res.size() == 0) {
            return null;
        }

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, res.size());
        for (String str : res) {
            VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(str));
        }
        return blockBuilder;
    }
}
