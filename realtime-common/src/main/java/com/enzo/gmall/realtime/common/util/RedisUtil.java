package com.enzo.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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

    private static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    // 向redis写入维度数据
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接写入的key
        String key = getKey(tableName, id);
        // 写入数据到Redis
        jedis.setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String ping = jedis.ping();
        System.out.println(ping);
        closeJedis(jedis);
    }
}
