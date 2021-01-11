package control.influx;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;

/**
 * @author wb.lixinlin
 * @date 2020/12/31
 */
public class TestConnect {

    public static void main(String[] args) {
//        influxDB = InfluxDBFactory.connect("http://192.168.137.103:8086", "root", "root");
//        influxDB.setDatabase("light_net");

        InfluxDB connect = InfluxDBFactory.connect("http://192.168.137.103:8086", "root", "root");
        connect.setDatabase("light_net");
        QueryResult query = connect.query(new Query("select * from card_info"));

        List<QueryResult.Result> results = query.getResults();


        results.forEach(a->{
            a.getSeries().forEach(b->{
                b.getValues().forEach(c->{
                    c.forEach(d-> System.out.print(d+"\t"));
                    System.out.println();
                });
            });
        });

    }
}
