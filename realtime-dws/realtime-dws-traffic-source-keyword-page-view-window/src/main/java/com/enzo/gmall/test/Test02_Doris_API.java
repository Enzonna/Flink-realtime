package com.enzo.gmall.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

public class Test02_Doris_API {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点设置（如果处理的是无界数据，必须开启检查点）
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

//        // TODO 3. 从Doris表中读取数据
//        DorisOptions.Builder builder = DorisOptions.builder()
//                .setFenodes("hadoop102:7030")
//                .setTableIdentifier("test.table1")
//                .setUsername("root")
//                .setPassword("aaaaaa");
//
//        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
//                .setDorisOptions(builder.build())
//                .setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDeserializer(new SimpleListDeserializationSchema())
//                .build();
//
//        DataStreamSource<List<?>> stream1 = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
//        stream1.print();


        // TODO 4. 向Doris表中写入数据
//        DataStreamSource<String> source = env
//                .fromElements(
//                        "{\"siteid\": \"550\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}");
//        Properties props = new Properties();
//        props.setProperty("format", "json");
//        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
//
//        DorisSink<String> sink = DorisSink.<String>builder()
//                .setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
//                        .setFenodes("hadoop102:7030")
//                        .setTableIdentifier("test.table1")
//                        .setUsername("root")
//                        .setPassword("aaaaaa")
//                        .build())
//                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
//                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
//                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
//                        .setDeletable(false)
//                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
//                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
//                        .setMaxRetries(3)
//                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
//                        .build())
//                .setSerializer(new SimpleStringSerializer())
//                .build();
//        source.sinkTo(sink);


        String[] fields = {"siteid", "citycode", "username", "pv"};
        DataType[] dataTypes = {
                DataTypes.INT(),
                DataTypes.SMALLINT(),
                DataTypes.STRING(),
                DataTypes.BIGINT()
        };
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        SingleOutputStreamOperator<RowData> source = env
                .fromElements(
                        "{\"siteid\": \"3000\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}",
                        "{\"siteid\": \"5000\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}",
                        "{\"siteid\": \"6000\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}"
                )
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String json) throws Exception {
                        JSONObject obj = JSON.parseObject(json);
                        GenericRowData rowData = new GenericRowData(4);
                        rowData.setField(0, obj.getIntValue("siteid"));
                        rowData.setField(1, obj.getShortValue("citycode"));
                        rowData.setField(2, StringData.fromString(obj.getString("username")));
                        rowData.setField(3, obj.getLongValue("pv"));
                        return rowData;
                    }
                });

        DorisSink<RowData> sink = DorisSink.<RowData>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("hadoop102:7030")
                        .setTableIdentifier("test.table1")
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(RowDataSerializer.builder()
                        .setType(LoadConstants.JSON)
                        .setFieldNames(fields)
                        .setFieldType(dataTypes)
                        .build())
                .build();
        source.sinkTo(sink);


        env.execute();
    }
}
