package com.ti.wy.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author wb.lixinlin
 * @date 2020/12/10
 */
public class TableReadFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> stringDataSource = env.readTextFile("E:\\testdata\\order_info.txt");

        stringDataSource.writeAsText("E:\\testdata\\order_info_write.txt");

        env.setParallelism(1);
        env.execute();


        /*long count = stringDataSource.map(a -> new Tuple1(a)).returns(Types.TUPLE(Types.STRING))
                .count();

        System.out.println(count);*/


    }
}
