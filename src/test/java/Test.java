import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author wb.lixinlin
 * @date 2020/12/10
 */
public class Test {

    public static void main(String[] args) throws Exception {

        /*System.out.println((int)Math.floor(Math.random() * 10));

         *//*String dt = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.ofInstant(Instant.ofEpochSecond(System.currentTimeMillis() / 1000), ZoneId.systemDefault()));
        System.out.println(dt);*//*

        String data = "{\"id\":\"14-8-3\",\"method\":\"user.event.trigger.post\",\"params\":{\"time\":1608540113867,\"value\":{\"area\":14,\"cluster\":8,\"number\":3,\"uuid\":\"00-00-00-00-00-00-00-00-00-00-00-00\",\"nwk_id\":\"C4-21\",\"device_name\":\"2edd3bde-2c5c-4ae3-b3df-83152993dc11\",\"trig_time\":\"1608540113\"}},\"version\":\"1.0\"}";
        String s = new String(Base64.getEncoder().encode(data.getBytes()), StandardCharsets.UTF_8);
        System.out.println(s);*/

//        System.out.println(TimeUnit.MINUTES.toMillis(1));

        HashMap<String, String> map = new HashMap<>();
        String a = map.get("a");

        if (a != null) {
            System.out.println(a);
        }else{
            System.out.println("is null");
        }

    }


    private static void soutNum() {
        int i = 0;


        while (true) {
            if (i == 7) {
                return;
            }
            i++;
            System.out.println(i);
        }

    }
}
