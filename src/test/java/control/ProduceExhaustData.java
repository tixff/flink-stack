package control;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

/**
 * @author wb.lixinlin
 * @date 2020/12/23
 */
public class ProduceExhaustData {

    public static void main(String[] args) throws Exception {

        String brokers = "192.168.137.101:9092";
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Object, String> producer = new KafkaProducer<Object, String>(props);
        String data = "{\"id\":\"14-%d-%d\",\"method\":\"user.event.trigger.post\",\"params\":{\"time\":1608540113867,\"value\":{\"area\":14,\"cluster\":%d,\"number\":%d,\"uuid\":\"00-00-00-00-00-00-00-00-00-00-00-00\",\"nwk_id\":\"%s\",\"device_name\":\"2edd3bde-2c5c-4ae3-b3df-83152993dc11\",\"trig_time\":\"1608540113\"}},\"version\":\"1.0\"}";
        String trigger_data = "{\"productKey\":\"xA707NOn1Lh1D4r\",\"deviceName\":\"2edd3bde-2c5c-4ae3-b3df-83152993dc11\",\"topic\":\"xA707NOn1Lh1D4r/2edd3bde-2c5c-4ae3-b3df-83152993dc11/user/event/trigger/post\",\"messageId\":\"2e78abee-5a89-4305-b688-1ed2208b9348\",\"content\":\"%s\",\"timestamp\":1608540155276,\"topicId\":\"0NXnXGxOmpFGEYz\",\"topicType\":0,\"qos\":1}";


        while (true) {
            int cluster = (int) Math.ceil(Math.random() * 10);
            int number = (int) Math.ceil(Math.random() * 10);
            int i = (int) Math.ceil(Math.random() * 10) % 2;
            String nwk_id = i > 0 ? "C4-21" : "C4-D1";
            String format = String.format(data, cluster, number, cluster, number,nwk_id);
            String content = new String(Base64.getEncoder().encode(format.getBytes()), StandardCharsets.UTF_8);
            String jsonData = String.format(trigger_data, content);
            ProducerRecord<Object, String> iot_test = new ProducerRecord<Object, String>("iot_test", jsonData);
            producer.send(iot_test);
            producer.flush();
            System.out.println(format);
            Thread.sleep(500);
        }
    }
}
