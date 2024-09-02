package com.enzo.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.TableProcessDim;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Synchronize data from stream to HBase
 */
public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hBaseConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConnection = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConnection);
    }

    // Synchronize data to HBase
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup2, Context context) throws Exception {
        JSONObject jsonObj = tup2.f0;
        TableProcessDim tableProcessDim = tup2.f1;
        // Gets the type of operation on the business database - dimension table
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        // Gets the name of the dimension table
        String sinkTable = tableProcessDim.getSinkTable();
        // Gets the rowKey of the dimension table
        String sinkRowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        if ("delete".equals(type)) {
            // delete
            HBaseUtil.deleteRow(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKey);

        } else {
            // insert/update/bootstrap-insert
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKey, sinkFamily, jsonObj);
        }
    }
}
