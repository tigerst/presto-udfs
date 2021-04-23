package com.presto.udfs.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.presto.udfs.model.ChinaIdArea;
import org.apache.commons.lang.StringUtils;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConfigUtils {

    /**
     * presto-udfs配置
     */
    private Properties props = new Properties();

    private RedissonClient redissonClient = null;

    /**
     * day tye map
     */
    private RMap<String, String> dayMap;

    /**
     * china id area map
     */
    private RMap<String, String> areaMap;

    /**
     * hdfs prefix path
     */
    private String hdfsPrePath;

    private ConfigUtils() {
        //初始化配置
        initialConfig();
    }

    private static class SingletonInstance {
        private static final ConfigUtils INSTANCE = new ConfigUtils();
    }

    public static ConfigUtils getInstance(){
        return ConfigUtils.SingletonInstance.INSTANCE;
    }

    /**
     * 初始化配置
     * @return
     */
    private Properties initialConfig()  {
        Closer closer = Closer.create();
        try {
            System.out.println("Start to initial config.");
            InputStream inputStream = ConfigUtils.class.getResourceAsStream("/redis.properties");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            closer.register(bufferedReader);
            //加载配置
            props.load(bufferedReader);
            //创建redis client
            redissonClient = RedisFactory.create(props);
            //日期类型：date <---> type
            dayMap = redissonClient.getMap(props.getProperty("redis.day.type"));
            if (dayMap.isEmpty()) {
                //为空时，同步默认配置到redis
                try {
                    for (String line : readFile("/china_day_type.config")) {
                        String[] results = line.split("\t", 2);
                        dayMap.put(results[0], results[1]);
                    }
                } catch (IOException e) { }
            }
            //地区编码：code <---> address
            areaMap = redissonClient.getMap(props.getProperty("redis.city.area.code"));
            if (areaMap.isEmpty()) {
                //为空时，同步默认配置到redis
                ObjectMapper mapper = new ObjectMapper();
                for (String line : readFile("/china_p_c_a.config")) {
                    String[] results = line.split("\t", 4);
                    areaMap.put(results[0], mapper.writeValueAsString(new ChinaIdArea(results[1], results[2], results[3])));
                }
            }
            //hdfs地址前缀
            String ns = (String)redissonClient.getBucket(props.getProperty("redis.dfs.ns")).get();
            if (StringUtils.isBlank(ns)) {
                redissonClient.getBucket(props.getProperty("redis.dfs.ns")).set("tigercluster");
            }
            hdfsPrePath = String.format("hdfs://%s/", redissonClient.getBucket(props.getProperty("redis.dfs.ns")).get());
            System.out.println("Initial config successfully.");
        } catch (IOException e) {
            System.out.println("Initial config error.");
        } finally {
            try {
                closer.close();
            } catch (IOException e) {}
        }
        return props;
    }

    /**
     * 读文件
     * @param fileName
     * @return
     * @throws IOException
     */
    private List<String> readFile(String fileName) throws IOException {
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

    public Properties getProps() {
        return props;
    }

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    /**
     * 根据日期查询类型
     * @param date
     * @return
     */
    public String getDateType(String date) {
        return dayMap.get(date);
    }

    /**
     * 根据code查询地址
     * @param code
     * @return
     */
    public String getArea(String code){
        return areaMap.get(code);
    }

    public String getHdfsPrePath() {
        return hdfsPrePath;
    }

    public static void main(String[] args) {
        ConfigUtils.getInstance();
    }

}
