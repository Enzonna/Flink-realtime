package com.enzo.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis工具类
 */
public class RedisUtil {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop102", 6379, 10000);
    }

    // 获取Jedis
    public static Jedis getJedis() {
        System.out.println("获取Jedis");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    // 关闭Jedis
    public static void closeJedis(Jedis jedis) {
        System.out.println("关闭Jedis");
        if (jedis != null) {
            jedis.close();
        }
    }


    // 从redis读取维度数据
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        // 拼接查询的Redis的key
        String key = getKey(tableName, id);
        // 根据key查询Redis
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            // 缓存命中
            return JSON.parseObject(dimJsonStr);
        }
        return null;
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    // 向redis写入维度数据
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接写入的key
        String key = getKey(tableName, id);
        // 写入数据到Redis
        jedis.setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
    }


    // 获取异步操作Redis的客户端对象
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        System.out.println("获取异步操作Redis客户端");
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        return redisClient.connect();
    }

    // 关闭异步操作Redis的客户端对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            asyncRedisConn.close();
            System.out.println("关闭异步操作Redis客户端❌❌❌");
        }
    }

    // 以异步的方式从Redis中读取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> asyncRedisConns, String tableName, String id) {
        // 拼接查询的Redis的key
        String key = getKey(tableName, id);
        // 根据key查询Redis
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if (StringUtils.isNotEmpty(dimJsonStr)) {
                // 缓存命中
                return JSON.parseObject(dimJsonStr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    // 以异步的方式向Redis写入数据
    public static void writeDimAsync(StatefulRedisConnection<String, String> asyncRedisConns, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接写入的key
        String key = getKey(tableName, id);
        // 写入数据到Redis
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();
        asyncCommands.setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
    }
}
