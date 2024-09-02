package com.enzo.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.bean.TableProcessDwd;
import com.enzo.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class FlinkSinkUtil {
    // get kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("xxx")
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }


    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
                                JSONObject jsonObj = tup2.f0;
                                TableProcessDwd tableProcessDwd = tup2.f1;
                                String topic = tableProcessDwd.getSinkTable();
                                return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
                            }
                        }
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("xxx")
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 +"")
                .build();
        return kafkaSink;
    }
}
