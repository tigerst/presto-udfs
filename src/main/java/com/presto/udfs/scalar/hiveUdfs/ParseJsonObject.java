package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.json.*;
import com.presto.udfs.utils.JsonUtil;
import io.airlift.slice.Slice;


import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import static io.airlift.slice.Slices.utf8Slice;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;

public class ParseJsonObject {
//    @Description( "_FUNC_(String json, String key) - 本函数用于从指定的json中返回key指定的数值， key可以为json中的key,也支持JsonPath, "
//            + "\n  _FUNC_(String json, String key,boolean True) - 本函数用于从指定的json中返回key指定的数值，传入参数True值true可以打印堆栈信息, key可以为json中的key,也支持JsonPath")
//    @ScalarFunction("parse_json_object")
//    @SqlType()

    // 自定义的 JsonPathCustom 无法使用

    /*
    @ScalarFunction("parse_json_object")
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice jsonExtractScalar(@SqlType("varchar(x)") Slice json, @SqlType(CustomJsonPathType.NAME) JsonPathCustom jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction("parse_json_object")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractScalar_key(@SqlType(StandardTypes.JSON) Slice json, @SqlType("varchar(x)") Slice key)
    {
        return JsonExtract.extract(json, new JsonPathCustom(key.toStringUtf8()).getScalarExtractor());
    }*/


    private ParseJsonObject(){

    }

    @ScalarFunction("parse_json_object")
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice jsonExtractScalar(@SqlType("varchar(x)") Slice json, @SqlType("varchar(x)") Slice key)
    {
        if (json == null || key == null) {
            return null;
        }
        try {
            return JsonExtract.extract(json, new JsonPathCustom(key.toStringUtf8()).getScalarExtractor());
        } catch (Exception e) {
            return null;
        }
    }

    @ScalarOperator(OperatorType.CAST)
    @SqlType(CustomJsonPathType.NAME)
    @LiteralParameters("x")
    public static JsonPathCustom castVarcharToJsonPathCustom(@SqlType("varchar(x)") Slice pattern)
    {
        return new JsonPathCustom(pattern.toStringUtf8());
    }


    @ScalarFunction("parse_json_object_javatype")
    @SqlNullable
    //@LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharJsonExtractScalar_2(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(CustomJsonPathType.NAME) JsonPathCustom jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

//    @ScalarFunction(value = "parse_json_object",alias = {"get_json_object"})
    @ScalarFunction(value = "get_json_object")
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice jsonExtractScalar_key_2(@SqlType("varchar(x)") Slice json, @SqlType("varchar(x)") Slice key)
    {
        /**
        if(key.toStringUtf8().startsWith("$.")){
            return JsonExtract.extract(json, new JsonPathCustom(key.toStringUtf8()).getScalarExtractor());
        }else {
            return JsonExtract.extract(json, new JsonPathCustom("$."+key.toStringUtf8()).getScalarExtractor());
        }**/
        Slice s = null;
        try{
            s = Slices.utf8Slice(JsonTools.getJsonValue(json.toStringUtf8(),key.toStringUtf8()));
        }catch (Exception e){
            return null;
        }
        return  s;
    }


    @ScalarFunction(value = "parse_json_array")
    @SqlNullable
    @SqlType("array(varchar)")
    public static Block jsonExtractScalar_array(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice key)
    {
        /**
        if(key.toStringUtf8().startsWith("$.")){
            return JsonExtract.extract(json, new JsonPathCustom(key.toStringUtf8()).getScalarExtractor());
        }else {
            return JsonExtract.extract(json, new JsonPathCustom("$."+key.toStringUtf8()).getScalarExtractor());
        }
         **/
        if (json == null) {
            return null;
        }

        if(json.toStringUtf8().length()<2){
            return null;
        }
        if(("$."+key.toStringUtf8()).length()<2){
            return null;
        }


        String temp= json.toStringUtf8().replaceAll("\\x22", "\"");
        String array = JsonTools.getJsonValue(temp,key.toStringUtf8());
        Long length = jsonArrayLength(Slices.utf8Slice(array));
        if (length == null) {
            return null;
        }

//        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, length.intValue());
        List<String> res = new ArrayList<String>();
        try{
            res=JsonTools.getJsonArray(array);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        for(String str:res){
            VARCHAR.writeSlice(blockBuilder,Slices.utf8Slice(str));
        }
        return blockBuilder;

    }

    @ScalarFunction("parse_json_array")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtract_yy(@SqlType(StandardTypes.VARCHAR) Slice json) {
        Long length = jsonArrayLength(json);
        if (length == null) {
            return null;
        }
//        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = varcharJsonArrayGet(json, i);
            VARCHAR.writeSlice(blockBuilder, content);
        }
        return blockBuilder.build();
    }

    @ScalarFunction("json_array_extract")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        Long length = jsonArrayLength(json);
        if (length == null) {
            return null;
        }
//        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length.intValue());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = varcharJsonArrayGet(json, i);
            Slice result = varcharJsonExtract(content, jsonPathSlice);
            if (result == null) {
                blockBuilder.appendNull();
            } else {
                VARCHAR.writeSlice(blockBuilder, result);
            }
        }
        return blockBuilder.build();
    }

    @ScalarFunction("json_array_extract_scalar")
    @Description("extract json array value by given jsonPath.")
    @SqlType("array(varchar)")
    public static Block jsonArrayExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        Long length = jsonArrayLength(json);
        if (length == null) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, length.intValue());
        for (int i = 0; i < length; i++) {
            Slice content = varcharJsonArrayGet(json, i);
            Slice result = varcharJsonExtractScalar(content, jsonPathSlice);
            if (result == null) {
                blockBuilder.appendNull();
            } else {
                VARCHAR.writeSlice(blockBuilder, result);
            }
        }
        return blockBuilder.build();
    }

    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);
    private static final JsonFactory MAPPING_JSON_FACTORY = new MappingJsonFactory().disable(CANONICALIZE_FIELD_NAMES);


    private static Slice varcharJsonExtract(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        JsonPathCustom jsonPath = new JsonPathCustom(jsonPathSlice.toStringUtf8());
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    private static Slice varcharJsonExtractScalar(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice jsonPathSlice) {
        JsonPathCustom jsonPath = new JsonPathCustom(jsonPathSlice.toStringUtf8());
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }


    private static Long jsonArrayLength(@SqlType(StandardTypes.JSON) Slice json) {
        try (JsonParser parser = JsonUtil.createJsonParser(JSON_FACTORY, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }
            long length = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return length;
                }
                parser.skipChildren();

                length++;
            }
        } catch (IOException e) {
            return null;
        }
    }

    private static Slice varcharJsonArrayGet(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.BIGINT) long index) {
        return jsonArrayGet(json, index);
    }

    private static Slice jsonArrayGet(@SqlType(StandardTypes.JSON) Slice json, @SqlType(StandardTypes.BIGINT) long index) {
        // this value cannot be converted to positive number
        if (index == Long.MIN_VALUE) {
            return null;
        }

        try (JsonParser parser = JsonUtil.createJsonParser(MAPPING_JSON_FACTORY, json)) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return utf8Slice(tokens.get(0));
                    }

                    return null;
                }

                String arrayElement;
                if (token == START_OBJECT || token == START_ARRAY) {
                    arrayElement = parser.readValueAsTree().toString();
                } else {
                    arrayElement = parser.getValueAsString();
                }

                if (count == index) {
                    return arrayElement == null ? null : utf8Slice(arrayElement);
                }

                if (tokens != null) {
                    tokens.add(arrayElement);

                    if (count >= index * -1) {
                        tokens.remove(0);
                    }
                }

                count++;
            }
        } catch (IOException e) {
            return null;
        }
    }





}