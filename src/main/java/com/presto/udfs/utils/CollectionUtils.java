package com.presto.udfs.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CollectionUtils {

    public static Set<String> array2Set(String[] strs){
        Set<String> set = new HashSet<>();
        int len = strs.length;
        if (len>0) {
            Arrays.stream(strs).forEach(str -> set.add(str));
        }
        return set;
    }

}
