package com.presto.udfs.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.Properties;

/**
 * @ClassName: RedisFactory.java
 *
 * @Description: RedissonClient是线程安全的，不需要每次都shutdown，这是一个耗时操作，在进程结束回收时shutdown即可！
 *              jedis是线程不安全的（主要原因是Connection.outputStream\inputStream），所以需要每次close，归还到对象池
 *
 * @Author: Tiger
 *
 * @Date: 2019/11/12
 */
public class RedisFactory {

    private static Cache<String, RedissonClient> cache = Caffeine.newBuilder().maximumSize(3).build();


    /**
     * cache住，单例化
     *
     * @param conf 根据redis的connectionString作为key，key不同，则创建不同的redisClient，并cache
     * @return
     */
    public static RedissonClient create(Properties conf) {
        final String key = conf.getProperty("redis.connectString");
        final String password = conf.getProperty("redis.password");
        final int database = NumberUtils.toInt(conf.getProperty("redis.database"), 0);
        System.out.println("Redis[key=" + key + ", password=" + password + ", database=" + database+"]");
        return cache.get(key, address -> {
            Config config = new Config();
            SingleServerConfig singleConfig = config.useSingleServer();
            singleConfig.setAddress(address);
            if(StringUtils.isNotBlank(password)){
                singleConfig.setPassword(password);
            }
            singleConfig.setDatabase(database);
            // 使用设定的SingleServerConfig构造redisClient
            return Redisson.create(config);
        });
    }

}
