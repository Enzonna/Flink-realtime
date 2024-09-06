package com.enzo.gmall.realtime.dws.app;

import com.enzo.gmall.realtime.common.base.BaseSQLApp;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.SQLUtil;
import com.enzo.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // TODO 1. 注册分词函数到表执行环境
        tEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // TODO 2. 从页面日志事实表中读取数据，创建动态表，并指定Watermark的生成策略以及提取事件时间字段
        tEnv.executeSql("create table page_log(" +
                " common map<string, string>, " +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window"));
        // tEnv.executeSql("select page from page_log").print();


        // TODO 3. 过滤出搜索行为
        Table searchTable = tEnv.sqlQuery("select " +
                "page['item'] fullword, " +
                "et " +
                "from page_log " +
                "where page['last_page_id'] ='search' and page['item_type']='keyword' and page['item'] is not null ");
        // searchTable.execute().print();
        tEnv.createTemporaryView("search_table", searchTable);


        // TODO 4. 分词，并将分词的结果和原表的字段进行关联
        Table splitTable = tEnv.sqlQuery("select keyword, et " +
                "from search_table, lateral table(ik_analyze(fullword)) t(keyword) ");
        // splitTable.execute().print();
        tEnv.createTemporaryView("split_table", splitTable);


        // TODO 5. 分组、开窗、聚合计算
        Table resTable = tEnv.sqlQuery("select \n" +
                "    DATE_FORMAT(TUMBLE_START(et, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(TUMBLE_END(et, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    DATE_FORMAT(TUMBLE_START(et, INTERVAL '10' second), 'yyyy-MM-dd') cur_date,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count \n" +
                "    from split_table  group by keyword,TUMBLE(et, INTERVAL '10' second);");
        resTable.execute().print();


        // TODO 6. 将聚合的结果写到Doris表中
        //6.1 创建动态表和要写入的Doris表进行映射
        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                ")" + SQLUtil.getDorisDDL("dws_traffic_source_keyword_page_view_window"));
        //6.2 写入
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");


    }
}
