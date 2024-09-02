package com.enzo.gmall.realtime.common.util;

import com.enzo.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    // 获取kafka连接器相关的连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }


    // 获取hbase连接器相关的连接属性
    public static String getHBaseDDL(String tableName) {
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //获取Upser-kafka连接器相关连接属性
    public static String getUpsertKafka(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
