package com.enzo.gmall.realtime.dwd.db.split.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.bean.TableProcessDwd;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.FlinkSinkUtil;
import com.enzo.gmall.realtime.common.util.FlinkSourceUtil;
import com.enzo.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 事实表动态分流
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1.对流中数据进行类型转换并做简单的ETL  jsonStr->jsonObj  得到是主流业务数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }
                    }
                }
        );
        // jsonObjDS.print("jsonObjDS");

        // 2.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource
                = FlinkSourceUtil.getMySqlSource("gmall_config", "table_process_dwd");
        // 2.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS
                = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        // mysqlStrDS.print("mysqlStrDS");

        // 2.3 对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String s) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(s);
                        //获取对配置表进行的操作的类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcessDwd = null;
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据,需要从before属性中获取删除前的配置
                            tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            //从配置表中读取,向配置表中添加一条配置,对配置表信息进行了修改  都是从after属性中获取最终信息
                            tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcessDwd.setOp(op);
                        return tableProcessDwd;
                    }
                }
        );
        // tpDS.print();

        // TODO 3.广播配置流---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // TODO 4.将非广播流(主流业务数据)和广播流(配置流配置信息)进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);

        // TODO 5.对关联后的数据进行处理---process  经过处理后，得到的是需要动态分流的事实表数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> realDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));

        // TODO 6.将流中数据写到kafka主题
        // realDS.print();
        realDS.sinkTo(FlinkSinkUtil.getKafkaSink());

    }
}
