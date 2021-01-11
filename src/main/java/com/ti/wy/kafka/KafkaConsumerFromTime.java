package com.ti.wy.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author wb.lixinlin
 * @date 2020/12/10
 */
public class KafkaConsumerFromTime {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.75.101:9092");
        props.put("max.messages", "2");
        props.put("group.id", "group2");
//    	props.put("auto.commit.enable", "false");
//    	props.put("auto.commit.interval.ms", "1000"); //设置enable.auto.commit意味着偏移是通过config auto.commit.interval.ms控制的频率自动提交的。
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

//    	consumer.subscribe(Arrays.asList(topic));//过滤话题
        String startTime = "2020-12-10 11:25:00";//起始时间点
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long stime = sdf.parse(startTime).getTime();
        //存放主题分区信息
        List<PartitionInfo> partitionSet = consumer.partitionsFor("iot_test");

        //topicparition 和 开始时间对应的偏移量
        Map<TopicPartition, Long> offsetMapAll = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionSet) {
            //存放主题信息和时间戳
            Map<TopicPartition, Long> startmap = new HashMap<>();
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            startmap.put(topicPartition, stime);
            //给consumer指定分区
            consumer.assign(Collections.singletonList(topicPartition));
            //获得该分区的起始偏移量
            Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(startmap);
            if (offsetMap.get(topicPartition) == null) {
                continue;
            }
            offsetMapAll.put(topicPartition, offsetMap.get(topicPartition).offset());
        }


        if (offsetMapAll.size() > 0) {
//            consumer.assign(offsetMapAll.keySet());
            for (TopicPartition tp : offsetMapAll.keySet()) {
                consumer.seek(tp, offsetMapAll.get(tp));//设置分区的起始偏移量
            }
            int cnt = 0;
            //轮询读取消息
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
//        			TimeUnit.SECONDS.sleep(1);

                    LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(record.timestamp() / 1000), ZoneId.systemDefault());
                    String date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(localDateTime);
                    System.out.printf("date  = %s offset = %d, partition = %s , key = %s, value = %s%n", date, record.offset(), record.partition(), record.key(), record.value());
                    System.out.println(record.key() + "\t" + record.partition());
                    cnt++;
                }
                System.out.println("总数:" + cnt);
            }
        }

    }
}
