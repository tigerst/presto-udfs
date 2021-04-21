package com.presto.udfs.utils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.presto.udfs.model.ChinaIdArea;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfigUtils {

    public static List<String> loadFile(String fileName) throws IOException {
        ArrayList<String> strings = Lists.newArrayList();
        Closer closer = Closer.create();
        try {
            InputStream inputStream = ConfigUtils.class.getResourceAsStream(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            closer.register(bufferedReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
                    continue;
                }
                strings.add(line);
            }
        } catch (IOException e) {
            System.out.println("loadFile {} error. error is {}." + fileName);
            throw e;
        } finally {
            closer.close();
        }

        return strings;
    }

    public static Map<String, ChinaIdArea> getIdCardMap() {
        String fileName = "/china_p_c_a.config";
        Map<String, ChinaIdArea> map = Maps.newHashMap();
        try {
            List<String> list = loadFile(fileName);
            for (String line : list) {
                String[] results = line.split("\t", 4);
                map.put(results[0], new ChinaIdArea(results[1], results[2], results[3]));
            }
        } catch (IOException e) {
            System.out.println("get china id card map error. error is {}.");
            return map;
        }

        return map;
    }

    public static Map<String, String> getDayMap() {
        String fileName = "/china_day_type.config";
        Map<String, String> map = Maps.newHashMap();
        try {
            List<String> list = loadFile(fileName);
            for (String line : list) {
                String[] results = line.split("\t", 2);
                map.put(results[0], results[1]);
            }
        } catch (IOException e) {
            System.out.println("get day map error. error is {}.");
            return map;
        }

        return map;
    }
}
