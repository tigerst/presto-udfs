package com.presto.json;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class JsonTools {

    private static ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }


    /**
     * @param json
     * @param key
     * @return 从json 中解析到的key 参数
     * @throws IOException
     */
    public static String getJsonValue(String json, String key) {
        String res = JsonPath.read(json, key).toString();
        return res;
    }


    public static List<String> getJsonKeys(String json) throws IOException {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        HashMap<String, Object> o = mapper.readValue(json, typeRef);
        List<String> res = new ArrayList<>(o.keySet());

        /*List<String> res=new ArrayList<String>();
        String[] keys= JSONObject.getNames(new JSONObject(json));
        for(String k:keys){
            res.add(k);
        }*/
//		java.util.Collections.sort(res);
        return res;
    }

    /**
     * 从json 总解析 数组参数key，返回参数值的列表
     *
     * @param json
     * @param key
     * @return
     * @throws IOException
     */
    public static List<String> getJsonArray(String json, String key) {
        List<Object> val = JsonPath.read(json, key);
        List<String> res = new ArrayList<>();
        for (Object obj : val) {
            res.add(obj.toString());
        }
        return res;
    }

    public static List<String> getJsonArray(String json) throws IOException {
        List<String> res = new ArrayList<>();
        JsonNode jsonNode = mapper.readTree(json);
        if (jsonNode.isArray()) {
            ArrayNode arrNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrNode.size(); i++) {
                res.add(arrNode.get(i).toString());
            }
        }
        /*
         JSONArray arr = new JSONArray(json) ;
        List<String> res = new ArrayList<String>();
        for(int i=0; i<arr.length(); i++){
            res.add(arr.getString(i));
        }
        return res;
        */
        return res;
    }

    /**
     * 迭代json 里的数组参数arrName, 取每个元素的key值组成列表
     *
     * @param json
     * @param arrName
     * @param key
     * @return
     * @throws IOException
     */
    public static List<String> iterJson(String json, String arrName, String key) throws IOException {
//        JSONObject obj = new JSONObject(json);
//        JSONArray arr = obj.getJSONArray(arrName);
//        List<String> res = new ArrayList<>();
//        for (int i = 0; i < arr.length(); i++) {
//            res.add(arr.getJSONObject(i).getString(key));
//        }
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> o = mapper.readValue(json, typeRef);
        Object arr = o.get(arrName);
        if (Objects.isNull(arr)) {
            return new ArrayList<>();
        }
        return ((List<Map<String, Object>>) arr).stream().map(m -> (String) m.get(key)).collect(Collectors.toList());
    }



//    public static void main(String[] argc) throws Exception{
//        String aa = JsonTools.getJsonValue("{\"id\":\"1\",\"endTime\":\"2021-04-20 17:08\"}","$.endTime");
//        System.out.println(aa);
//    }
}
