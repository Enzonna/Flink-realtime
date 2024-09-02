package com.enzo.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.bean.TableProcessDim;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.FlinkSourceUtil;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import com.enzo.gmall.realtime.dim.function.DimSinkFunction;
import com.enzo.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Dim层实现
 *
 * 需要启动的进程：zk,kafka,hdfs,maxwell,hbase,DimApp
 *
 * 开发流程：
 *          环境准备
 *          检查点相关的设置
 *          从kafka主题中读取数据
 *          对主流中数据进行类型转换并进行简单清洗ETL jsonStr -> jsonObj
 *          ---------------------主流-------------------------------------
 *          使用FlinkCDC读取配置表数据
 *          对配置流数据进行类型转换    jsonStr -> TableProcessDim实体类对象
 *              op = d  从before中获取对应的配置信息
 *              op != d  从after中获取对应的配置信息
 *          根据配置流信息到HBase中建表或者删表
 *              op = c || op = r    建表
 *              op = d    删表
 *              op = u    先删后建
 *          ----------------------配置流------------------------------------
 *          广播配置流   broadcast
 *          用非广播流（主流业务数据）和广播流（配置流配置信息）进行关联  connect
 *          对关联后的数据进行处理     process
 *              class TableProcessFunction extends BroadcastProcessFunction{
 *                  open:预加载配置信息到程序中，解决了主流数据先到，广播流数据后到的问题
 *                  processElement: 处理主流数据
 *                      根据当前处理的业务数据库表的表名到广播状态以及ConfigMap中获取对应的配置信息
 *                      如果配置信息不为空，说明是维度表，将维度数据向下游传递
 *                          在向下游传递数据前，过滤掉不需要传递的字段
 *                          在向下游传递数据前，补充type属性
 *                          传递的内容：Tuple2<dataJsonObj,TableProcessDim>
 *                  processBroadcastElement: 处理广播流数据
 *                      op = d      从广播状态以及ConfigMap中删除对应的配置信息
 *                      op != d     将配置信息更新到广播状态以及ConfigMap中
 *              }
 *
 *     将维度数据写到HBase表中
 *          class DimSinkFunction extends RichSinkFunction{
 *              invoke:
 *                  type=delete  从HBase表中删除一条数据
 *                  type!= delete(insert、update、bootstrap-insert)  向HBase表中put一条数据
 *          }
 *
 * 执行流程（以修改品牌维度表中的一条数据为例）
 *      将需要启动的进程都启动起来
 *      当程序启动的时候，配置表中的配置信息会被加载到ConfigMap以及广播状态
 *      修改了品牌表中的某条数据
 *      binlog会记录修改操作
 *      maxwell会从binlog中读取到变化的数据，并将其封装为json字符串发送到kafka的topic_db主题中
 *      DimApp应用会从topic_db主题中读取数据并对数据进行处理
 *      在处理主流数据的时候，会从广播状态或者ConfigMap中获取对应的配置信息，判断是不是维度数据
 *      如果是维度数据发送到下游同步到HBase中
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {

        //🍵🍵🍵
        new DimApp().start(10001, 4, "dim_app", "topic_db");

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1. 对流中数据进行类型转换并进行简单清洗ETL  jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // TODO 2. 使用FlinkCDC读取配置表数据
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        // TODO 3. 根据配置表中的信息到HBase中建标或者删表
        tpDS = createHBaseTable(tpDS);

        // TODO 4. 将配置流进行广播 --  broadcast,将主流和广播流进行关联  -- connect,将关联后的数据进行处理 --  process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);

        // TODO 5. 将维度数据写入HBase
        writeToHBase(dimDS);
    }


    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.print();
        dimDS.addSink(new DimSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        // TODO 9. 将主流和广播流进行关联  -- connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);


        // TODO 10. 将关联后的数据进行处理 --  process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hBaseConnection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConnection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConnection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        // 获取对配置表操作
                        String op = tableProcessDim.getOp();

                        String sinkTable = tableProcessDim.getSinkTable();
                        String[] families = tableProcessDim.getSinkFamily().split(",");

                        if ("c".equals(op) || "r".equals(op)) {
                            // 从配置表中读取或者向配置表中新增数据，在Hbase中执行建表操作
                            HBaseUtil.createHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);

                        } else if ("d".equals(op)) {
                            // 从配置表中删除了一条配置信息，在Hbase中执行删表操作
                            HBaseUtil.dropHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);

                        } else {
                            // 对配置表的信息进行了更新操作，先从Hbase中删除对应的表，再新建表
                            HBaseUtil.dropHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, families);

                        }
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // TODO 5. 使用FlinkCDC从MySQL中读取配置信息
        // 5.1 创建MysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall_config", "table_process_dim");

        // 5.2 读取数据，封装成流
        DataStreamSource<String> mysqlStrDS =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);

        // TODO 6. 对配置数据进行转换    jsonStr ->  实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        // 将json字符串转换为jsonObj
                        JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                        // 获取对配置表进行的操作的类型
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            // 说明从配置表中删除了一条数据，从before属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            // 说明从配置表中进行了读取、新增、修改操作，从after属性中获取配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        return tpDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if ("gmall".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        return jsonObjDS;
    }
}
