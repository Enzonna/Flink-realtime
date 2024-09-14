package com.enzo.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.enzo.gmall.realtime.common.function.DimAsyncFunction;
import com.enzo.gmall.realtime.common.util.DateFormatUtil;
import com.enzo.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 省份粒度下单聚合统计
 * zk,kf,maxwell,hdfs,hbase,redis,doris,DwdTradeOrderDetail
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        // TODO 1. 对流中数据进行类型转换并过滤空消息    jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );
        // jsonObjDS.print();
        // {"create_time":"2024-09-05 09:34:12","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
        // "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-09-05","user_id":"10279","province_id":"1","activity_id":"4",
        // "sku_name":"TCL 85Q6 85英寸 巨幕私人影院电视 4K超高清 AI智慧屏 全景全面屏 MEMC运动防抖 2+16GB 液晶平板电视机","id":"52903",
        // "order_id":"38072","split_activity_amount":"1199.9","split_total_amount":"10799.1","ts":1725845652}

        // TODO 2. 按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // TODO 3. 去重   状态➕抵消
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        // distinctDS.print();

        // TODO 4. 再次对流中数据进行类型转换    jsonObj -> 统计的实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = distinctDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                        String provinceId = jsonObject.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                        String orderId = jsonObject.getString("order_id");
                        Long ts = jsonObject.getLong("ts") * 1000;
                        Set orderIdSet = new HashSet<>();
                        orderIdSet.add(orderId);

                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderIdSet(orderIdSet)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
        // beanDS.print();


        // TODO 5. 指定Watermark的生成策略，以及提取事件时间字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderBean tradeProvinceOrderBean, long l) {
                                        return tradeProvinceOrderBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 6. 按照统计的维度进行分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = withWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        // TODO 7. 开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS
                = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // TODO 8. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> rsDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );
        rsDS.print();


        // TODO 9. 关联维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceInfoDS = AsyncDataStream.unorderedWait(
                rsDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withProvinceInfoDS.print();

        // TODO 10. 将关联的结果写到Doris中
        withProvinceInfoDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
