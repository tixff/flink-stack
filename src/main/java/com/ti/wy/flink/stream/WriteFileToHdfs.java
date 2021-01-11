package com.ti.wy.flink.stream;

import com.ti.wy.flink.table.TableApiTest;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;


/**
 * @author wb.lixinlin
 * @date 2020/12/14
 */
public class WriteFileToHdfs {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8888");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        environment.enableCheckpointing(10000);


        SingleOutputStreamOperator<String> map = environment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    int num = (int) (Math.random() * 100);
                    char ch = (char) ('A' + (int) (Math.random() * 26));
                    ctx.collect(String.valueOf(ch));
                }
            }

            @Override
            public void cancel() {

            }
        });


        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://dfscluster/tmp"), new SimpleStringEncoder<String>("UTF-8"))
                /**
                 * 设置桶分配政策
                 * DateTimeBucketAssigner--默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String element, Context context) {

                        String dt = "dt=" + DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(context.currentProcessingTime()), ZoneId.systemDefault()));
                        return dt;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                /**
                 * 有三种滚动政策
                 *  CheckpointRollingPolicy
                 *  DefaultRollingPolicy
                 *  OnCheckpointRollingPolicy
                 */
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
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(10)) //设置滚动间隔
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(5)) //设置不活动时间间隔
                                .withMaxPartSize(1024 * 1024) // 最大零件尺寸
                                .build())
                .withOutputFileConfig(new OutputFileConfig("a", ".txt"))
                .build();


        map.addSink(sink);


        environment.execute("write to hdfs");


    }


}
