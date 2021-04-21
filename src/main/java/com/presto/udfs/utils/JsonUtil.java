package com.presto.udfs.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    private JsonUtil() {
    }

    public static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException {
        return factory.createParser((InputStream) json.getInput());
    }

    public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output)
            throws IOException {
        return factory.createGenerator((OutputStream) output);
    }

    public static <T> T json2Obj(String str, Class<T> clazz) {
        try {
            return mapper.readValue(str, clazz);
        } catch (IOException e) {
            e.printStackTrace();
            return (T) null;
        }
    }

    public static <T> String obj2Json(T obj) {
        try {
            mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true) ;
            mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true) ;
            return mapper.writeValueAsString(obj);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 是否json字符串
     * @param jsonInString
     * @return
     */
    public static boolean isJSONString(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static Set<String> getJsonKeysSet(String str){
        Set<String> ret = Sets.newHashSet();
        getJsonKeys(str, ret);
        return ret;
    }

    public static List<String> getJsonKeysList(String str){
        List<String> ret = Lists.newArrayList();
        getJsonKeys(str, ret);
        return ret;
    }


    public static JsonNode getJsonNode(String str) {
        try {
            return mapper.readTree(str);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 获取json所有的同级key
     * @param str
     * @return
     */
    public static void getJsonKeys(String str, Collection collection){
        try {
            JsonNode jsonNode = mapper.readTree(str);
            getJsonKeys(jsonNode, collection);
        } catch (IOException e) {
        }
    }

    public static Set<String> getAllJsonKeysSet(String str){
        Set<String> ret = Sets.newHashSet();
        getAllJsonKeys(str, ret);
        return ret;
    }

    /**
     * 获取json字符串中所有的key
     * @param str
     * @return
     */
    public static void getAllJsonKeys(String str, Collection collection){
        try {
            JsonNode jsonNode = mapper.readTree(str);
            getJsonKeysRecursion(jsonNode, collection);
        } catch (IOException e) {
        }
    }

    private static void getJsonKeysRecursion(JsonNode jsonNode, Collection collection) {
        getJsonKeys(jsonNode, collection);
        Iterator<JsonNode> elements = jsonNode.elements();
        while (elements.hasNext()) {
            JsonNode next = elements.next();
            getJsonKeysRecursion(next, collection);
        }
    }

    public static void getJsonKeys(JsonNode jsonNode, Collection collection){
        Iterator<String> it = jsonNode.fieldNames();
        while (it.hasNext()) {
            collection.add(it.next());
        }
    }
}
