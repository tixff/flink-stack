package com.ti.wy.flink.table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wb.lixinlin
 * @date 2020/12/22
 */
public class ReadMysql {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        JDBCInputFormat format = JDBCInputFormat.buildJDBCInputFormat()
                //    .指定驱动名称
                .setDrivername("com.mysql.jdbc.Driver")
                //      url
                .setDBUrl("jdbc:mysql://localhost:3306/light_prod")
                .setUsername("root")
                .setPassword("root")
                .setQuery("select building,floor,section,area_type,remark,light_id,nwk_id from light_info")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                ))
                .finish();

        DataSource<Row> input = env.createInput(format);
//        input.print();


        BatchTableEnvironment batchTable = BatchTableEnvironment.create(env);
        StreamTableEnvironment streamTable = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        batchTable.sqlUpdate("CREATE TABLE light_info (\n" +
                " building varchar," +
                "`floor` varchar," +
                "section varchar," +
                "area_type varchar," +
                "remark varchar," +
                "light_id varchar," +
                "nwk_id varchar" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://localhost:3306/light_prod',\n" +
                "  'connector.table' = 'light_info',\n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver', \n" +
                "  'connector.username' = 'root',\n" +
                "  'connector.password' = 'root',\n" +
                "  'connector.read.partition.column' = 'light_id',\n" +
                "  'connector.read.partition.num' = '50',\n" +
                "  'connector.read.partition.lower-bound' = '500',\n" +
                "  'connector.read.partition.upper-bound' = '1000',\n" +
                "  'connector.read.partition.num' = '50', \n" +
                "  'connector.read.partition.lower-bound' = '500', \n" +
                "  'connector.read.partition.upper-bound' = '1000',\n" +
                "  'connector.read.fetch-size' = '100'" +
                ")");

        Table table = batchTable.sqlQuery("select * from light_info");
        table.printSchema();


        batchTable.toDataSet(table, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING)).print();


    }
}
