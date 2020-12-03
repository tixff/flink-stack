package com.ti.wy.flink.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;

/**
 * @author wb.lixinlin
 * @date 2020/12/3
 */
public class TableApiTest {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> strings = new ArrayList<>();

        strings.add("aaa");
        strings.add("bbb");
        strings.add("ccc");

        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSource<String> dataset = env.fromCollection(strings);



        Table table = tEnv.fromDataSet(dataset);

        table.printSchema();
    }
}
