package com.enzo.gmall.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.enzo.gmall.realtime.common.base.BaseApp;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.enzo.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.enzo.gmall.realtime.common.util.DateFormatUtil;
import com.enzo.gmall.realtime.common.util.FlinkSinkUtil;
import com.enzo.gmall.realtime.dws.util.KeywordUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;


public class DwsTrafficSourceKeywordPageTest extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageTest().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> mapDS = kafkaSource.map(JSON::parseObject);
        //mapDS.print();


        SingleOutputStreamOperator<JSONObject> filterDS = mapDS
                .filter(
                        jsonObj -> "search".equals(jsonObj.getJSONObject("page").getString("last_page_id"))
                                && "keyword".equals(jsonObj.getJSONObject("page").getString("item_type"))
                );
        //filterDS.print();


        SingleOutputStreamOperator<keywordBean> processDS = filterDS.process(
                new ProcessFunction<JSONObject, keywordBean>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, keywordBean>.Context ctx, Collector<keywordBean> out) throws Exception {
                        String keyword = jsonObject.getJSONObject("page").getString("item");
                        Long ts = jsonObject.getLong("ts");
                        Set<String> words = KeywordUtil.analyze(keyword);
                        for (String word : words) {
                            keywordBean bean = new keywordBean();
                            bean.setKeyword(word);
                            bean.setKeyword_count(1L);
                            bean.setTs(ts);
                            out.collect(bean);
                        }
                    }
                }
        );
        // processDS.print();

        SingleOutputStreamOperator<keywordBean> withWatermarkDS = processDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<keywordBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<keywordBean>() {
                            @Override
                            public long extractTimestamp(keywordBean keywordBean, long l) {
                                return keywordBean.getTs();
                            }
                        })
        );


        KeyedStream<keywordBean, String> keyedDS = withWatermarkDS.keyBy(keywordBean::getKeyword);
        // keyedDS.print();

        WindowedStream<keywordBean, String, TimeWindow> windowDS = keyedDS
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<keywordBean> rsDS = windowDS
                .reduce(
                        new ReduceFunction<keywordBean>() {
                            @Override
                            public keywordBean reduce(keywordBean keywordBean, keywordBean t1) throws Exception {
                                keywordBean.setKeyword_count(keywordBean.getKeyword_count() + t1.getKeyword_count());
                                //System.out.println("keywordBean = " + keywordBean);
                                return keywordBean;
                            }
                        },
                        new WindowFunction<keywordBean, keywordBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow timeWindow, Iterable<keywordBean> iterable, Collector<keywordBean> collector) throws Exception {
                                keywordBean keywordBean = iterable.iterator().next();
                                keywordBean.setStt(DateFormatUtil.tsToDateTime(timeWindow.getStart()));

                                keywordBean.setEdt(DateFormatUtil.tsToDateTime(timeWindow.getEnd()));
                                keywordBean.setCur_date(DateFormatUtil.tsToDate(timeWindow.getStart()));

                                collector.collect(keywordBean);
                            }
                        }
                );

        rsDS.print();

        rsDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_source_keyword_page_view_window"));


    }
}


@Data
@NoArgsConstructor
@AllArgsConstructor
class keywordBean {
    String stt;
    String edt;
    String cur_date;

    private String keyword;
    private Long keyword_count;

    @JSONField(serialize = false)
    Long ts;
}