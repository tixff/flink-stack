package com.ti.wy.flink.sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author wb.lixinlin
 * @date 2020/12/2
 */
public class SqlStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableStream = StreamTableEnvironment.create(env);


        tableStream.sqlUpdate("CREATE TABLE flink_test_table (\n" +
                "  `productKey` STRING,\n" +
                "  deviceName STRING\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.11',\n" +
                "  'connector.topic' = 'flink_test',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'connector.properties.zookeeper.connect' = '192.168.75.101:2181',\n" +
                "  'connector.properties.bootstrap.servers' = '192.168.75.101:9092',\n" +
                "  'update-mode' = 'append',\n" +
                "  'format.type' = 'json'\n" +
                ")");

        Table table = tableStream.sqlQuery("select * from flink_test_table");

        tableStream.toAppendStream(table, Types.TUPLE(Types.STRING, Types.STRING)).print();

        tableStream.execute("sql kafka source stream ");


    }
}
