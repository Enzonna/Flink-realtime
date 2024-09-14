package com.enzo.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.TableProcessDim;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import com.enzo.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * Synchronize data from stream to HBase
 */
public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hBaseConnection;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConnection = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConnection);
        RedisUtil.closeJedis(jedis);
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

        // 🍵🍵🍵2024.9.7加，如果维度数额发生了变化，需要将Redis中缓存的维度清除掉
        if ("delete".equals(type) || "update".equals(type)) {
            jedis.del(RedisUtil.getKey(sinkTable, sinkRowKey));
        }

    }
}
