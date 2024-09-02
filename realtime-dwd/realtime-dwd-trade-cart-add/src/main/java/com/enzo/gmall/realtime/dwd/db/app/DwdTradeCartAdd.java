package com.enzo.gmall.realtime.dwd.db.app;

import com.enzo.gmall.realtime.common.base.BaseSQLApp;
import com.enzo.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // 1. 从kafka的topic_db主题中读取数据，创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        // tableEnv.executeSql("select * from topic_db").print();

        // 2. 过滤出加购行为
        Table cartInfoTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,       \n" +
                "    `data`['user_id'] user_id,       \n" +
                "    `data`['sku_id'] sku_id,          \n" +
                "    if(`type` = 'insert',`data`['sku_num'],CAST((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS string)) sku_num,       \n" +
                "\tts\n" +
                "from topic_db\n" +
                "where `table` = 'cart_info' \n" +
                "and `type` = 'insert'\n" +
                "or (`type` = 'update' and `old`['sku_num'] is not null \n" +
                "and CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT))");
        // table.execute().print();


        // 3. 将加购数据写到kafka主题中
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + " (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  sku_num string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + Constant.TOPIC_DWD_TRADE_CART_ADD + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        cartInfoTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
