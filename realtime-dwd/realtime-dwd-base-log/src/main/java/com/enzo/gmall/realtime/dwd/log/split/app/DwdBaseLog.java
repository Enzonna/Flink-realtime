package com.enzo.gmall.realtime.dwd.log.split.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.util.DateFormatUtil;
import com.enzo.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;


/**
 * Log diversion
 * <p>
 * Flink保证一致性：
 *      Source：数据可重放
 *          KafkaSource -> KafkaSourceReader -> offsetsToCommit(会保存到算子状态中)
 *          流处理框架（Flink）:检查点
 *          Sink：幕等，事务
 *              KafkaSink保证一致性：
 *                  开启检查点
 *                  设置一致性级别为精准一次    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *                  设置事务id的前缀           .setTransactionalIdPrefix("xxx")
 *                  设置事务超时时间           .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
 *                  在消费端，设置消费的隔离级别为读已提交
 *
 * 需要启动的进程:
 *     zk、kafka、flume、DwdBaseLog
 */
public class DwdBaseLog extends BaseApp {

    private final String ERR = "err";
    private final String START = "start";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", "topic_log");
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // kafkaStrDS.print("kafka✅✅:");

        // TODO 1. Perform type conversion on streaming data and perform simple ETL     jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        //jsonObjDS.print("etl✅✅:");

        // TODO 2. Repairs 新老访客 label
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);
//        fixedDS.print("fixed:");

        // TODO 3. Splitting, putting different types of logs into different streams
        Map<String, DataStream> streamMap = splitStream(fixedDS);

        // TODO 4. Write different data to the topic of Kafka
        writeToKafka(streamMap);
    }

    private void writeToKafka(Map<String, DataStream> streamMap) {
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        // 错误日志-错误测输出流，启动日志-启动测输出流，曝光日志-曝光测输出流，动作日志-动作测输出流，页面日志-主流
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        // 🍵🍵🍵errTag
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            // 将错误日志放入错误测输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        // 🍵🍵🍵startTag
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // 启动日志
                            // 将启动日志放入启动测输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 🍵🍵🍵页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // 🍵🍵🍵曝光日志
                            JSONArray displaysArr = jsonObj.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                // 遍历all曝光日志
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                                    // 定义一个新的json，用于封装当前遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    // 将曝光数据写到曝光测输出流中
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }


                            // 🍵🍵🍵动作日志
                            JSONArray actionsArr = jsonObj.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                // 遍历所有动作日志
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject actionJsonObj = actionsArr.getJSONObject(i);
                                    // 定义一个新的json，用于封装当前遍历出来的动作数据
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    // 将动作数据写到动作测输出流中
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }


                            // 🍵🍵🍵页面日志
                            // 将页面数据发送到主流
                            out.collect(jsonObj.toJSONString());

                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);


        //errDS.print("err:✅");
        //pageDS.print("page:✅");
        //startDS.print("start:✅");
        //displayDS.print("display:✅");
        //actionDS.print("action:✅");

        Map<String, DataStream> splitMap = new HashMap<>();
        splitMap.put("err", errDS);
        splitMap.put("start", startDS);
        splitMap.put("display", displayDS);
        splitMap.put("action", actionDS);
        splitMap.put("page", pageDS);

        return splitMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        // 2.1 Group by device ID
        KeyedStream<JSONObject, String> keyedDS =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 2.2 Using Flink's state programming for repair
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    // Declaration status
                    private ValueState<String> lastVisitDateState;

                    /**
                     * 状态初始化
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 状态描述器
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDate", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            // 如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                // 如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改
                                lastVisitDateState.update(curVisitDate);
                            }else {
                                // 如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            // 如果 is_new 的值为 0
                            // 如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。
                            // 当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，
                            // 只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                long yesterDayTs = ts - 24 * 60 * 60 * 1000;
                                String yesterDay = DateFormatUtil.tsToDate(yesterDayTs);
                                lastVisitDateState.update(yesterDay);
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
//        fixedDS.print();
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 1.1 Define 测出流 label
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        // 1.2 Conversion and ETL operations
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            // If no exceptions occur during the conversion process,
                            // it indicates that this is standard JSON and continues to be passed downstream
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // If an exception occurs during the conversion process,
                            // it indicates that this is not standard JSON and belongs to dirty data,
                            // which should be included in the 测输出流
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        jsonObjDS.print("standard data🍵");

        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        jsonObjDS.getSideOutput(dirtyTag).print("dirty data🍵");


        // 1.3 Send the dirty data of 测出流 to the Kafka topic
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
