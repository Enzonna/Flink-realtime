package com.enzo.gmall.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 3. 从FlinkCDC中读取数据

        // 3.1 创建MysqlSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.t_user")
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        // 3.2 读取数据，封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mysqlStrDS.print();
        env.execute("Print MySql Snapshot + Binlog");

    }
}
