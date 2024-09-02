package com.enzo.gmall.realtime.test;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * FlinkAPI提供了基于时间的join
 *      基于窗口的join:滚动窗口，滑动窗口
 *      基于状态的join:intervaljoin
 *  以上的join有一定的局限性，只支持内连接，不支持外连接，如果在FlinkAPI中要实现外连接的效果，需要手动调用connect来实现
 *
 *                          左表                      右表
 *  内连接                 onCreateAndWrite        onCreateAndWrite
 *  左外连接               onReadAndWrite          onCreateAndWrite
 *  右外连接               onCreateAndWrite        onReadAndWrite
 *  全外连接
 *
 */
public class Flink01_SQL_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态保留时间❌❌❌
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 1. Read employee data from the specified network port and create a dynamic table
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("hadoop102", 8888)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String s) throws Exception {
                        String[] fieldArr = s.split(",");
                        return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                    }
                });
        // Convert the stream into a table
        tableEnv.createTemporaryView("emp", empDS);

        // 2. Read department data from the specified network port and create a dynamic table
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("hadoop102", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String s) throws Exception {
                        String[] fieldArr = s.split(",");
                        return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                    }
                });
        // Convert the stream into a table
        tableEnv.createTemporaryView("dept", deptDS);

        // 3. 内连接
        // ❌❌❌一定要注意❌❌❌：在普通的内外连接中，FlinkSQL底层会为参与连接的两张表各自维护一个状态，用于存放表中的数据
        // 默认情况下，状态永不失效，如果数据量过大，状态越来越大，会导致内存溢出！！！
        // 在生产环境中，如果使用了普通的内外连接，一定要设置状态的保留时间 -> tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        // tableEnv.executeSql("select e.empno, e.ename, d.deptno, d.dname from emp e join dept d on e.deptno = d.deptno").print();

        // 4. 左外连接
        // 如果左表数据先到，右表数据后到，会产生3条数据
        // 左表   null    +I
        // 左表   null    -D
        // 左表   右表     +I
        // 这样的动态表转化的流称之为回撤流

        // 如果将↑结果写入kafka主题的话，kafka主题会收到3条信息
        // 左表   null
        // null
        // 左表   右表

        // 如果使用FlinkSQL的方式从kafka主题中读取数据的话，空消息会自动处理过滤
        // 如果使用FlinkAPI的方式从kafka主题中读取数据的话，空消息不会自动处理过滤，需要手动过滤
        // 另外，处理空消息时，第1条数据和第3条数据，属于重复数据，在DWS层聚合统计时，需要对其进行去重

        // tableEnv.executeSql("select e.empno, e.ename, d.deptno, d.dname from emp e left join dept d on e.deptno = d.deptno").print();

        // 5. 右外连接
        // tableEnv.executeSql("select e.empno, e.ename, d.deptno, d.dname from emp e right join dept d on e.deptno = d.deptno").print();

        // 6. 全外连接
        // tableEnv.executeSql("select e.empno, e.ename, d.deptno, d.dname from emp e full join dept d on e.deptno = d.deptno").print();

        // 7. Write the results of external connections into the Kafka topic
        // 7.1 创建动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno integer,\n" +
                "  ename string,\n" +
                "  deptno integer,\n" +
                "  dname string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");\n");

        // 7.2 write in
        tableEnv.executeSql("insert into emp_dept select e.empno, e.ename, d.deptno, d.dname from emp e left join dept d on e.deptno = d.deptno");




        env.execute();
    }

}
