package com.ti.wy.flink.stream;

import com.ti.wy.flink.bean.OrderData;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import scala.Tuple5;

/**
 * @author wb.lixinlin
 * @date 2020/12/10
 */
public class ReadFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> datasource = streamEnv.readTextFile("E:\\testdata\\order_info.txt");

        StreamTableEnvironment steamTable = StreamTableEnvironment.create(streamEnv);

        SingleOutputStreamOperator<Tuple5> map = datasource.map(a -> {
            String[] strs = a.split("\t");
            return new Tuple5(strs[0], strs[1], strs[2], strs[3], strs[4]);
        }).returns(new TypeHint<Tuple5>() {
            @Override
            public TypeInformation<Tuple5> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        map.print();

        SingleOutputStreamOperator<OrderData> order_stream = datasource.map(a -> {
            String[] strs = a.split("\t");
            return new OrderData(strs[0], Double.parseDouble(strs[1]), strs[2], strs[3], strs[4]);
        })/*.returns(TypeInformation.of(OrderData.class))*/;

        Table table = steamTable.fromDataStream(order_stream);

        Table money = table.select("sum(money)");

        DataStream<Tuple2<Boolean, Double>> dataStream = steamTable.toRetractStream(money, Types.DOUBLE);


        dataStream.print().setParallelism(1);


        streamEnv.execute("test");
    }


}


