package com.ti.wy.flink.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/*
 *
 * @author lixinlin
 * @date 2020/11/04
 *
 **/
public class CardInfo {


    private static final Logger LOG = LoggerFactory.getLogger(CardInfo.class);


    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /*Configuration configuration = new Configuration();
        configuration.setString("rest.port", "5555");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);*/

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.137.101:9092");
        properties.setProperty("group.id", "iot_light_group0001");
        properties.setProperty("auto.offset.reset", "earliest");//earliest  latest


        FlinkKafkaConsumer010<ArrayList<String>> myConsumer = new FlinkKafkaConsumer010<ArrayList<String>>(
                "iot_test",
                new DeserializationSchema() {
                    @Override
                    public ArrayList<String> deserialize(byte[] message) throws IOException {
                        ArrayList<String> datas = new ArrayList<>();

                        try {
                            ByteBuf buf = Unpooled.buffer();
                            buf.markWriterIndex();
                            buf.writeBytes(message);

                            //59, 0, -91
                            if (message.length > 3 && message[0] == 59 && message[1] == 0 && message[2] == -91) {
                                // 略过头部3个字节
                                buf.skipBytes(3);
                                // 字符串容器,总共15个字节
                                byte[] box = new byte[15];
                                buf.readBytes(box);
                                // 刷卡设备868626047774974
                                long gateway = Long.parseLong(new String(box));
                                String deviceNo = String.format("%015d", gateway);
                                // 包数4
                                int size = buf.readUnsignedByte();
                                // 信号强度23
                                int rssi = buf.readUnsignedByte();
                                // 定位状态;3
                                int gpsState = buf.readByte();
                                // 定位经度;104.03609
                                long longitude = buf.readUnsignedIntLE();
                                float longitudeFloat = (float) longitude / 100000;
                                String longitudeStr = String.valueOf(longitudeFloat);
                                // 定位纬度;30.62609
                                long latitude = buf.readUnsignedIntLE();
                                float latitudeFloat = (float) latitude / 100000;
                                String latitudeStr = String.valueOf(latitudeFloat);

                                Long now = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
                                for (int i = 0; i < size; i++) {
                                    // 卡号3645646313
                                    long cardNo = buf.readUnsignedIntLE();
                                    long timestamp = buf.readUnsignedIntLE() * 1000L;

                                    String data = "{\"card_no\":\"%s\",\"create_time\":\"%s\",\"receive_time\":\"%s\",\"serial_no\":\"%s\",\"rssi\":\"%s\",\"gps_state\":\"%s\",\"latitude\":\"%s\",\"longtitude\":\"%s\"}";
                                    String formatdata = String.format(data, cardNo, timestamp, now, deviceNo, rssi, gpsState, latitudeStr, longitudeStr);
                                    datas.add(formatdata);
                                    LOG.info("card_info:" + formatdata);
                                }
                            }
                        } catch (Exception e) {
                            LOG.error("二进制数据解析失败", e);
                        }
                        return datas;
                    }

                    @Override
                    public boolean isEndOfStream(Object nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation getProducedType() {
                        return Types.LIST(Types.STRING);
                    }
                },
                properties);


        SingleOutputStreamOperator<String> cardData = env.addSource(myConsumer)
                .filter(a -> a.size() > 0)
                .flatMap((ArrayList<String> a, Collector<String> b) -> a.forEach(b::collect))
                .returns(Types.STRING);


        cardData.addSink(new RichSinkFunction<String>() {

            InfluxDB influxDB = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                try {
                    influxDB = InfluxDBFactory.connect("http://192.168.137.103:8086", "root", "root");
                    influxDB.setDatabase("light_net");
                } catch (Exception e) {
                    LOG.error("get influxdb connetion error:", e);
                }
            }

            @Override
            public void close() throws Exception {
                try {
                    if (influxDB != null) {
                        influxDB.close();
                    }
                } catch (Exception e) {
                    LOG.error("close  influxdb error:", e);
                }
            }

            @Override
            public void invoke(String value, Context context) throws Exception {

                JSONObject data = JSON.parseObject(value);
                HashMap<String, String> tagMap = new HashMap<>();
                HashMap<String, Object> filedMap = new HashMap<>();
                Point.Builder card_info = Point.measurement("card_info");


                tagMap.put("card_no", data.getString("card_no"));
                tagMap.put("serial_no", data.getString("serial_no"));
                tagMap.put("rssi", data.getString("rssi"));
                tagMap.put("gps_state", data.getString("gps_state"));

                filedMap.put("latitude", Float.parseFloat(data.getString("latitude")));
                filedMap.put("longtitude", Float.parseFloat(data.getString("longtitude")));


                card_info.fields(filedMap);
                card_info.tag(tagMap);

                Point point = card_info.build();

                influxDB.write(point);
            }
        });


        try {
            env.execute("card_info");
        } catch (Exception e) {
            LOG.error("job submit error", e);

            e.printStackTrace();
        }
    }
}
