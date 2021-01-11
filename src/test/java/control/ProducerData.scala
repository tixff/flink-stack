package control

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 *
 * @author wb.lixinlin
 * @date 2020/10/23
 */
object ProducerData {

  def main(args: Array[String]): Unit = {
    val brokers = "192.168.137.101:9092"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

    val value = "{\"productKey\":\"Ql5L4BYQO9tk6LE\",\"deviceName\":\"df071629-327a-4f70-916d-ee36e4360bf7\",\"topic\":\"Ql5L4BYQO9tk6LE/df071629-327a-4f70-916d-ee36e4360bf7/user/report/info/update\",\"messageId\":\"5fbe82b3-f72d-4eed-8fee-6f0f6791ba93\",\"content\":\"CnsiYWRkcmVzc0NvZGUiOiJBMDAzVzM3MjUxMzMwMTI5IiwidGVtcCI6IjAyMy4wIiwiaHVtIjoiMDU0LjQifQ==\",\"timestamp\":1603354521510,\"topicId\":\"mmMXJWPmvXugGx\",\"topicType\":0,\"qos\":1}"
    /*val by = Array[Byte](59, 0, -91, // 前三个字节可以跳过：前两个字节是长度，第三个字节是命令
      56, 54, 56, 54, 50, 54, 48, 52, 55, 55, 55, 52, 57, 55, 52, // 15个字节是刷卡机序列号；解析结果：868626047774974
      4, // 1个字节；size包数；是指有多少个卡号上来；解析结果：
      23, // 1个字节；信号强度；解析结果：
      3, // 1个字节；定位状态；1失败；2成功；解析结果：
      25, -65, -98, 0, // 4个字节；定位经度；解析结果：
      81, -69, 46, 0, // 4个字节；定位纬度；解析结果：
      // 根据size循环卡号
      -23, 37, 76, -39, // 4个字节； 卡号;解析结果：3645646313
      -58, 106, -86, 95, // 4个字节；数据上报时间戳;解析结果：1605003974000
      -23, 37, 76, -39, // 4个字节； 卡号
      -58, 106, -86, 95, // 4个字节；数据上报时间戳
      -23, 37, 76, -39, -57, 106, -86, 95, -23, 37, 76, -39, -57, 106, -86, 95, // 4个字节；数据上报时间戳；
      61, 127) // crc16;2个校验字节；低位在前面)*/

    while (true) {

      Math.random()

      val by = Array[Byte](59, 0, -91, // 前三个字节可以跳过：前两个字节是长度，第三个字节是命令
        56, 54, 56, 54, 50, 54, 48, 52, 55, 55, 55, 52, 57, 55, 52, // 15个字节是刷卡机序列号；解析结果：868626047774974
        4, // 1个字节；size包数；是指有多少个卡号上来；解析结果：
        23, // 1个字节；信号强度；解析结果：
        3, // 1个字节；定位状态；1失败；2成功；解析结果：
        25, -65, (Math.random() * 100).toByte, 0, // 4个字节；定位经度；解析结果：
        81, -69, (Math.random() * 100).toByte, 0, // 4个字节；定位纬度；解析结果：
        // 根据size循环卡号
        -23, 37, 76, -39, // 4个字节； 卡号;解析结果：3645646313
        -58, 106, -86, 95, // 4个字节；数据上报时间戳;解析结果：1605003974000
        -23, 37, 76, -39, // 4个字节； 卡号
        -58, 106, -86, 95, // 4个字节；数据上报时间戳
        -23, 37, 76, -39, -57, 106, -86, 95, -23, 37, 76, -39, -57, 106, -86, 95, // 4个字节；数据上报时间戳；
        61, 127) // crc16;2个校验字节；低位在前面)
      val rcd = new ProducerRecord[Array[Byte], Array[Byte]]("iot_test", by)
      producer.send(rcd)
      producer.flush()
      //      producer.close()

      Thread.sleep(1000)
      println("send message")
    }


    print((Math.random() * 10000).toByte)
  }

}
