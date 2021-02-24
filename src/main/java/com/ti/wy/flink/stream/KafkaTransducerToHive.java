package com.ti.wy.flink.stream;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wb.lixinlin
 * @date 2021/2/24
 */
public class KafkaTransducerToHive {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransducerToHive.class);

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        /*Configuration configuration = new Configuration();
        configuration.setString("rest.port", "5555");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);*/

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "59.111.106.63:9092");
        properties.setProperty("group.id", "kafkaTransducertohive_group0001");
        properties.setProperty("auto.offset.reset", "earliest");
        String consumer_topic = "IOT-Az9271QXDlfN953";

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(
                consumer_topic,
                new SimpleStringSchema(),
                properties);

        DataStreamSource<String> source = env.addSource(myConsumer);

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://dfscluster/user/hive/warehouse/ods.db/transducer"), new SimpleStringEncoder<String>("UTF-8"))
                /**
                 * 设置桶分配政策
                 * DateTimeBucketAssigner--默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String element, Context context) {
                        long timestamp = Long.parseLong(JSON.parseObject(element).getString("timestamp"));
                        String dt = "dt=error";
                        if (Long.toString(timestamp).length() == 13) {
                            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                            dt = "dt=" + DateTimeFormatter.ofPattern("yyyyMMdd").format(localDateTime);
                        }

                        if (Long.toString(timestamp).length() == 10) {
                            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
                            dt = "dt=" + DateTimeFormatter.ofPattern("yyyyMMdd").format(localDateTime);
                        }

                        return dt;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .withRollingPolicy(
                        /**
                         * 滚动策略决定了写出文件的状态变化过程
                         * 1. In-progress ：当前文件正在写入中
                         * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
                         * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
                         *
                         * 观察到的现象
                         * 1.会根据本地时间和时区，先创建桶目录
                         * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
                         * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
                         *
                         */
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(60 * 60)) //设置滚动间隔
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(60)) //设置不活动时间间隔//
                                .withMaxPartSize(1024 * 1024 * 10) // 最大零件尺寸10m
                                .build())
                .withOutputFileConfig(new OutputFileConfig("transducer", ".txt"))
                .build();


        source.addSink(sink).setParallelism(1);

        try {
            env.execute("kafka Transducer write to hdfs");
        } catch (Exception e) {
            LOG.error("程序启动失败");
        }

    }
}
