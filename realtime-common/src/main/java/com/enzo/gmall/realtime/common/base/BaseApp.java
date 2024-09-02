package com.enzo.gmall.realtime.common.base;

import com.enzo.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The base class for all FlinkAPI programs
 * <p>
 * 模板方法设计模式：在父类中定义完成某个功能的核心算法骨架（步骤），但是某些步骤在父类中无法完成，需要延迟到子类中去实现
 * <p>
 * 好处：定义程序执行的模板；在不改变父类核心算法的骨架的前提下，每一个子类都可以有自己不同的实现
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // TODO 1. 基本环境准备 ✅✅固定写法✅✅
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // parallelism
        env.setParallelism(parallelism);

        // TODO 2. 检查点
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//
//        // 2.2 设置检查点超时时间
//        checkpointConfig.setCheckpointTimeout(60000L);
//
//        // 2.3 设置job取消后检查点是否保留
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        // 2.4 设置两个检查点之间的最小间隔
//        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
//
//        // 2.5 设置状态后端
//        // env.setStateBackend(new HashMapStateBackend());
//
//        // 2.6 设置检查点的存储路径
//        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck" + ckAndGroupId);
//
//        // 2.7 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 2.8 设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "enzo");

        // TODO 3. 从kafka主题中读取数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);

        // 3.3 消费数据，封装成流
        DataStreamSource<String> kafkaStrDS =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_source");

        // 4. Business logic processing
        handle(env, kafkaStrDS);

        // 5. Submit assignment
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource);
}
