package com.enzo.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.DateFormatUtil;
import com.enzo.gmall.realtime.common.util.HBaseUtil;
import com.enzo.gmall.realtime.common.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * sku粒度，下单聚合统计
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        // TODO 1. 对流中数据进行类型转换，并且过滤掉空消息
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            out.collect(JSONObject.parseObject(jsonStr));
                        }
                    }
                }
        );
        // jsonObjDS.print("🍵🍵🍵");

        // TODO 2. 按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 3. 去重
        /*
        方案一：状态 + 定时器    优点：出现重复，也只会向下游发送一条消息，不会膨胀 ❌❌❌缺点明显：不管数据是否重复，都要等5秒才会传递到下游，时效性比较差
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 状态
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        // 判断上条数据是否为空
                        if (lastJsonObj == null) {
                            // 不重复,将当前数据放到状态中
                            lastJsonObjState.update(jsonObject);
                            // 注册5s后执行的定时器
                            TimerService timerService = ctx.timerService();
                            long currentProcessingTime = timerService.currentProcessingTime();
                            timerService.registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            // 说明重复数据，用当前数据的聚合时间和状态中的聚合时间进行比较，将时间大的放进状态中
                            String ts1 = lastJsonObj.getString("聚合时间");
                            String ts2 = jsonObject.getString("聚合时间");
                            if (ts2.compareTo(ts1) >= 0) {
                                // 将当前这条数据放到状态中
                                lastJsonObjState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 当定时器触发时执行的方法
                        // 获取状态中的数据
                        JSONObject jsonObj = lastJsonObjState.value();
                        // 将数据向下游传递
                        out.collect(jsonObj);
                        // 清状态
                        lastJsonObjState.clear();
                    }
                }
        );
         */

        // 方案二：状态 + 抵消  缺点：重复，向下游发送三条消息，有数据膨胀的现象
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 状态
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            // 说明重复了，将状态中影响度量值的字段取反，传递到下游
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            // 取反
                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        // distinctDS.print("distinctDS");

        // TODO 4. 再次对流中数据进行转换 jsonObj -> 实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> orderBeanDS = distinctDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {

                        return TradeSkuOrderBean.builder()
                                .skuId(jsonObj.getString("sku_id"))
                                .originalAmount(jsonObj.getBigDecimal("split_original_amount"))
                                .couponReduceAmount(jsonObj.getBigDecimal("split_coupon_amount"))
                                .activityReduceAmount(jsonObj.getBigDecimal("split_activity_amount"))
                                .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                                .ts(jsonObj.getLong("ts") * 1000)
                                .build();
                    }
                }
        );
        // orderBeanDS.print("orderBeanDS");

        // TODO 5. 指定watermark生成策略以及提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> watermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeSkuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
        );

        // TODO 6. 按照统计的维度sku进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = watermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        // TODO 7. 开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS
                = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));


        // TODO 8. 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
        // reDS.print("reDS");


        // TODO 9. 关联sku维度
        /* ❌❌❌ 性能低,和hbase的连接过于平凡
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reDS.map(


                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 1. 根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        // 2. 根据维度的主键获取对应的维度的对象
                        JSONObject dimJsonObj
                                = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);
                        // 3. 将维度对象属性补充到流中的对象上
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));

                        return orderBean;
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                }
        );
        withSkuInfoDS.print("withSkuInfoDS");
        */

        // ✅✅✅ 优化1 旁路缓存
        SingleOutputStreamOperator<TradeSkuOrderBean> mapDS = reDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 1. 根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();

                        // 2. 根据维度的主键到redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            // 3. 如果在Redis中获取到了维度数据--直接将其返回（缓存命中）
                            System.out.println("从redis从获取数据");
                        } else {
                            // 4. 如果在redis中没有获取到维度数据，发送请求到Hbase中查询维度
                            dimJsonObj
                                    = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);

                            if (dimJsonObj != null) {
                                System.out.println("从hbase中获取数据");
                                // 5. 并将从Hbase中查询出的维度放到redis中缓存起来
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("❌❌❌有错误，去维度表里找数据！");
                            }
                        }
                        // 6. 将维度属性补充到流中对象上
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        }
                        return orderBean;
                    }
                }
        );
        mapDS.print("mapDS");


        // TODO 10. 关联spu维度
        // TODO 11. 关联tm维度
        // TODO 12. 关联Category3维度
        // TODO 13. 关联Category2维度
        // TODO 14. 关联Category1维度
        // TODO 15. 将关联结果写到Doris
    }
}
