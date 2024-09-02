package com.enzo.gmall.realtime.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_SQL_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 1. Read employee data from Kafka topics and create dynamic tables
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  empno string,\n" +
                "  ename string,\n" +
                "  deptno string,\n" +
                "  pt as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // tableEnv.executeSql("select * from emp").print();

        // 2. Read department data from Hbase and create dynamic tables
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                " deptno string,\n" +
                " info ROW<dname string>,\n" +
                " PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_dept',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181'\n" +
                ");");
        // tableEnv.executeSql("select deptno, dname from dept").print();

        // 3. Associate employees with departments
        // 不能使用普通的内外连接，因为状态的失效时间没有办法设置
        // 如果不设置，员工数据越来越大，如果设置，部门数据失效后将不再读取
        // 使用LookupJoin进行关联，lookup底层实现原理和普通的内外连接不同，底层不会给参与连接的两张表各自维护状态
        // 它是从左表进行驱动，当左表数据到来的时候，发送请求和右表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empno, e.ename, d.deptno, d.dname\n" +
                "FROM emp AS e\n" +
                "  JOIN dept FOR SYSTEM_TIME AS OF e.pt AS d\n" +
                "    ON e.deptno = d.deptno");
        joinedTable.execute().print();


        // 4. Write the associated results into the Kafka topic
        // Create dynamic tables and map topics to be written
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno string,\n" +
                "  ename string,\n" +
                "  deptno string,\n" +
                "  dname string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'second',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        // write in
        joinedTable.executeInsert("emp_dept");


    }
}
