package com.ti.wy.flink.table;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.ArrayList;
import java.util.stream.IntStream;

/**
 * @author wb.lixinlin
 * @date 2020/12/3
 */
public class TableApiTest {

    public static void main(String[] args) throws Exception {

        // ******************
        // FLINK STREAM QUERY
        // ******************
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        fsTableEnv.registerFunction("setstr", new CharDealDUF());

        fsTableEnv.registerFunction("tuple2str", new tupleto2DUF());

        fsTableEnv.registerFunction("selavg", new avgDUAF());


        // or TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);
        SingleOutputStreamOperator<Data> map = fsEnv.addSource(new SourceFunction<Tuple2<String, String>>() {
            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                while (true) {
                    int num = (int) (Math.random() * 100);
                    char ch = (char) ('A' + (int) (Math.random() * 26));
                    ctx.collect(new Tuple2<>(String.valueOf(ch), String.valueOf(num)));
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(1).map(new RichMapFunction<Tuple2<String, String>, Data>() {
            @Override
            public Data map(Tuple2<String, String> value) throws Exception {
                int indexOfThisSubtask = this.getRuntimeContext().getIndexOfThisSubtask();
                return new Data(value.f0, value.f1, String.valueOf(indexOfThisSubtask));
            }
        }).returns(Data.class);

        Table table = fsTableEnv.fromDataStream(map);

        table.printSchema();

        fsTableEnv.createTemporaryView("c_table", table);

        Table chara = table
                .select("*")
                .where("charas =='A' || charas=='Z'")
                .groupBy("charas")
                .select("charas,num.count,tasknum.count.distinct");

        Table table2 = table
                .select("charas,num")
                .joinLateral("tuple2str(charas) as newStr")
                .select("charas,newStr");

        Table table3 = table
                .select("charas,num,tasknum")
                .select("selavg(num,tasknum) as avgvalue");


//        fsTableEnv.toRetractStream(chara, Types.TUPLE(Types.STRING, Types.LONG, Types.LONG)).print();

//        fsTableEnv.toRetractStream(table1, Types.TUPLE(Types.STRING, Types.LONG)).print();

//        fsTableEnv.toRetractStream(table2, Types.TUPLE(Types.STRING, Types.STRING)).print();

//        fsTableEnv.toRetractStream(table3, Types.DOUBLE).print();
//
//        fsEnv.execute("stream table test");


        // ******************
        // FLINK BATCH QUERY
        // ******************

        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);


        Class[] types = IntStream.range(0, 3).mapToObj(i -> String.class).toArray(Class[]::new);


        DataSource<Tuple4<String, String, String, String>> data = fbEnv.readCsvFile("E:\\testdata\\pro.csv")
                .ignoreFirstLine()
                .includeFields("1111")
                .ignoreFirstLine().types(String.class, String.class, String.class, String.class);

        Table batchDataTable = fbTableEnv.fromDataSet(data);

        Table countTable  = batchDataTable.select("count(1) as total_num");
        countTable.printSchema();
        fbTableEnv.toDataSet(batchDataTable, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)).print();

        fbTableEnv.toDataSet(countTable, Types.TUPLE(Types.LONG)).print();



    }

    public static class CharDealDUF extends ScalarFunction {


        public String eval(String line) {
            return (line + "--@#--");
        }
    }

    public static class tupleto2DUF extends TableFunction<String> {


        @Override
        public TypeInformation<String> getResultType() {
            return Types.STRING;
        }


        public void eval(String line) {

            char c = line.toCharArray()[0];
            char d = (char) (c + 1);

            collect(String.valueOf(c) + d);

        }
    }

    public static class avgDUAF extends AggregateFunction<Double, Tuple2<Double, Long>> {

        @Override
        public Double getValue(Tuple2<Double, Long> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return new Tuple2<>(0d, 0l);
        }

        public void accumulate(Tuple2<Double, Long> accumulator, String iValue, String iWeight) {
            accumulator.f0 += Double.valueOf(iValue) * Double.valueOf(iWeight);
            accumulator.f1 += Long.valueOf(iWeight);
        }
    }

    public static class Data {
        String charas;
        String num;
        String tasknum;

        public Data() {

        }

        public Data(String chara, String num, String tasknum) {
            this.charas = chara;
            this.num = num;
            this.tasknum = tasknum;
        }

        public String getCharas() {
            return charas;
        }

        public void setCharas(String charas) {
            this.charas = charas;
        }

        public String getNum() {
            return num;
        }

        public void setNum(String num) {
            this.num = num;
        }

        public String getTasknum() {
            return tasknum;
        }

        public void setTasknum(String tasknum) {
            this.tasknum = tasknum;
        }
    }
}
