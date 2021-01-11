package com.ti.wy.flink;


import scala.math.Ordering;

import java.io.UnsupportedEncodingException;

/**
 * @author wb.lixinlin
 * @date 2020/12/4
 */
public class Test {

    public static void main(String[] args) throws UnsupportedEncodingException {

        byte[] crc = CRC("1-8-B");


        System.out.println(bin2HexStr(crc));

    }


    public static String bin2HexStr(byte[] bytes) {

        StringBuilder result = new StringBuilder();
        String hexStr = "0123456789ABCDEF";
        String hex = "";
        for (byte aByte : bytes) {
            //字节高4位
            hex = String.valueOf(hexStr.charAt((aByte & 0xF0) >> 4));
            //字节低4位
            hex += String.valueOf(hexStr.charAt(aByte & 0x0F));
            result.append(hex);  //+" "
        }
        return result.toString();
    }


    public static byte[] CRC(String data) {
        byte[] retdata = new byte[2];

        byte[] crcbuf = data.getBytes();
        //计算并填写CRC校验码
        int crc = 0xFFFF;
        int len = crcbuf.length;
        for (byte b : crcbuf) {
            byte i;
            crc = crc ^ b;
            for (i = 0; i < 8; i++) {
                int TT;
                TT = crc & 1;
                crc = crc >> 1;
                crc = crc & 0x7fff;
                if (TT == 1) {
                    crc = crc ^ 0xa001;
                }
                crc = crc & 0xffff;
            }
        }

        retdata[1] = (byte) (crc >> 8);
        retdata[0] = (byte) (crc & 0xff);

        return retdata;
    }
}
