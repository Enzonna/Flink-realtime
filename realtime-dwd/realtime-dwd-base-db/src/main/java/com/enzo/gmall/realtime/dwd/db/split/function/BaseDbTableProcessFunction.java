package com.enzo.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.TableProcessDwd;
import com.enzo.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private Map<String, TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //将配置表中的配置信息预加载到程序中
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySqlConnection = JdbcUtil.getMySqlConnection();
        String sql = "select * from gmall_config.table_process_dwd";
        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mySqlConnection, sql, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable, sourceType);
            configMap.put(key, tableProcessDwd);
        }
        JdbcUtil.closeMySqlConnection(mySqlConnection);
    }

    private String getKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }


    //处理主流业务数据
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        //获取当前处理这条数据对应的业务数据库表的表名
        String table = jsonObj.getString("table");
        //获取当前处理这条数据对应的业务数据库表的操作类型
        String type = jsonObj.getString("type");
        //拼接key
        String key = getKey(table, type);
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据key到广播状态以及configMap中获取对应的配置信息
        TableProcessDwd tableProcessDwd = null;

        if((tableProcessDwd = broadcastState.get(key)) != null
                || (tableProcessDwd = configMap.get(key)) != null){
            //如果获取的配置信息不为空，说明当前处理的数据是需要动态分流的事实表数据  将其中data部分以及配置对象封装为Tuple2传递到下游
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //在向下游传递数据前，过滤掉不需要传递的字段
            String sinkColumns = tableProcessDwd.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);
            //在向下游传递数据前，补充ts
            dataJsonObj.put("ts",jsonObj.getLong("ts"));
            out.collect(Tuple2.of(dataJsonObj,tableProcessDwd));
        }
    }

    // 过滤
    private void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }

    //处理广播流配置信息
    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        //获取对配置表进行的操作类型
        String op = tableProcessDwd.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = tableProcessDwd.getSourceTable();
        String sourceType = tableProcessDwd.getSourceType();
        String key = getKey(sourceTable, sourceType);
        if("d".equals(op)){
            //从配置表中删除了一条配置信息    从广播状态以及ConfigMap中删除对应的配置
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            //对配置表进行了读取、添加以及更新操作  将最新的信息放到广播状态以及ConfigMap中
            broadcastState.put(key,tableProcessDwd);
            configMap.put(key,tableProcessDwd);
        }
    }


}
