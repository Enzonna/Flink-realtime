package com.enzo.gmall.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01_Doris_SQL {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点设置（如果处理的是无界数据，必须开启检查点）
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);


//        // TODO 3. 从Doris表中读取数据
//        tEnv.executeSql("CREATE TABLE flink_doris (  " +
//                "    siteid INT,  " +
//                "    citycode SMALLINT,  " +
//                "    username STRING,  " +
//                "    pv BIGINT  " +
//                "    )   " +
//                "    WITH (  " +
//                "      'connector' = 'doris',  " +
//                "      'fenodes' = 'hadoop102:7030',  " +
//                "      'table.identifier' = 'test.table1',  " +
//                "      'username' = 'root',  " +
//                "      'password' = 'aaaaaa'  " +
//                ")  ");
//        tEnv.sqlQuery("select * from flink_doris").execute().print();


        // TODO 4. 向Doris表中写入数据
        tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")  ");

        tEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");
    }
}
