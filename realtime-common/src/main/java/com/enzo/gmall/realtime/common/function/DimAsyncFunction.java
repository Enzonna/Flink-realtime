package com.enzo.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.DimJoinFunction;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import com.enzo.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 发送异步请求进行维度关联的模板类
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private StatefulRedisConnection<String, String> asyncRedisConn;
    private AsyncConnection asyncHBaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncRedisConn = RedisUtil.getRedisAsyncConnection();
        asyncHBaseConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(asyncRedisConn);
        HBaseUtil.closeHBaseAsyncConnection(asyncHBaseConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象    有返回值，当前线程任务的返回值将作为下一个线程任务的参数进行传递
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        //发送异步请求到Redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(asyncRedisConn, getTableName(), getRowKey(obj));
                        return dimJsonObj;
                    }
                }
                //执行线程任务    有入参、有返回值
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            //如果从Redis中查询到了要关联的维度，直接向下游传递(缓存命中)
                            System.out.println("✅✅✅从Redis中获取到" + getTableName() + "的" + getRowKey(obj) + "维度");
                        } else {
                            //如果从Redis中没有查询到要关联的维度，发送异步请求到HBase中查询
                            dimJsonObj = HBaseUtil.readDimAsync(asyncHBaseConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(obj));
                            if (dimJsonObj != null) {
                                System.out.println("✅✅✅从HBase中获取到" + getTableName() + "的" + getRowKey(obj) + "维度");
                                //将从HBase中查询出来的数据放到Redis中缓存起来
                                RedisUtil.writeDimAsync(asyncRedisConn, getTableName(), getRowKey(obj), dimJsonObj);
                            } else {
                                System.out.println("❌❌❌没有获取到" + getTableName() + "的" + getRowKey(obj) + "维度");
                            }
                        }
                        return dimJsonObj;
                    }
                }
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if (dimJsonObj != null) {
                            //将维度属性补充到流中对象上
                            addDims(obj, dimJsonObj);
                        }
                        //将结果向下游传递
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        throw new RuntimeException("检查zk、kafka、maxwell、hdfs、hbase、redis是否正常启动\n" +
                "HBase中维度表是否对历史数据进行了同步\n");
    }
}
