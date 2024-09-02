package com.enzo.gmall.realtime.common.base;

import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    public void start(int port, int parallelism, String ck) {
        // TODO 1. 基本环境准备
        // 1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 设置并行度
        env.setParallelism(parallelism);

        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // TODO 2. 检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        /*// 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);

        // 2.3 设置job取消后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.4 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        // 2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 2.6 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());

        // 2.7 设置检查点存储路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck/" + ck);

        // 2.8 设置hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "enzo");*/

        // TODO 3. 业务处理
        handle(env, tableEnv);
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

    /**
     *  从kafka的topic_db主题中读取数据，创建动态表
     * @param tableEnv
     * @param topic
     */
    public void readOdsDb(StreamTableEnvironment tableEnv,String topic) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>,\n" +
                "  ts bigint,\n" +
                "  pt as proctime(),\n" +
                "  et as TO_TIMESTAMP_LTZ(ts,0),\n" +
                "  WATERMARK FOR et AS et\n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, topic));
        // tableEnv.executeSql("select * from topic_db").print();
    }


    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SQLUtil.getHBaseDDL("dim_base_dic"));
        // tableEnv.executeSql("select * from base_dic").print();
    }
}
