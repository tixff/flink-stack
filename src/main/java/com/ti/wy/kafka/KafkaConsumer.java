package com.ti.wy.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "59.111.106.63:9092");
        props.put("group.id", "CountryCounter4");
        props.put("auto.offset.reset", "earliest");//earliest  latest
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList("IOT-Ql5L4BYQO9tk6LE"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord record : records) {
                System.out.println("message:" + record.key() + "\t" + record.value().toString());

//                YarnJobMonitor.sendToMail(record.value().toString());
            }
        }
    }
}
