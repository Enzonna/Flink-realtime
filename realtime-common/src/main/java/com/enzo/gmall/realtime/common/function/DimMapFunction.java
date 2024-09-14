package com.enzo.gmall.realtime.common.function;


import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.DimJoinFunction;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import com.enzo.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * 旁路缓存维度关联 模板类
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimJoinFunction<T> {

    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public T map(T obj) throws Exception {
        // 1. 根据流中对象获取要关联的维度的主键
        String key = getRowKey(obj);

        // 2. 根据维度的主键到redis中获取维度数据
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);

        if (dimJsonObj != null) {
            // 3. 如果在Redis中获取到了维度数据--直接将其返回（缓存命中）
            System.out.println("从Redis从获取" + getTableName() + "表的" + key + "数据✅✅✅");
        } else {
            // 4. 如果在redis中没有获取到维度数据，发送请求到Hbase中查询维度
            dimJsonObj
                    = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), key, JSONObject.class, false);
            if (dimJsonObj != null) {
                System.out.println("从HBase从获取" + getTableName() + "表的" + key + "数据✅✅✅");
                // 5. 并将从Hbase中查询出的维度放到redis中缓存起来，方便下次使用
                RedisUtil.writeDim(jedis, getTableName(), key, dimJsonObj);
            } else {
                System.out.println("❌❌❌有错误，去" + getTableName() + "表里找" + key + "维度！");
            }
        }

        if (dimJsonObj != null) {
            // 6. 将维度属性补充到流中对象上
            addDims(obj, dimJsonObj);
        }

        return obj;
    }

}
