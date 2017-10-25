package com.wade.ops.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateUtil {

    /**
     * 将格式化的时间戳转成系统认识的long
     *
     * @param dateString
     * @return
     * @throws Exception
     */
    public static final long parse(String dateString) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = format.parse(dateString);
        return date.getTime();
    }

    /**
     * 找出两个时间戳的相同前缀
     *
     * @param starttime
     * @param endtime
     * @return
     */
    public static final String prefixMatch(String starttime, String endtime) {

        int i = 0;
        while (i < starttime.length()) {
            if (starttime.charAt(i) != endtime.charAt(i)) {
                break;
            }
            i++;
        }

        return starttime.substring(0, i);
    }


    public static final String past1000Second() {
        long timestamp = System.currentTimeMillis();
        System.out.println("now: " + timestamp);

        long past1000Sec = timestamp - 1000000;
        System.out.println("pre: " + past1000Sec);

        timestamp -= (timestamp % 1000000);
        String now = Long.toString(timestamp).substring(0, 7);
        System.out.println("nst: " + now);

        past1000Sec -= (past1000Sec % 1000000);
        String xxx = Long.toString(past1000Sec).substring(0, 7);
        System.out.println("pst: " + xxx);
        return now;
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(parse("2017-10-23 11:39:59"));
//        System.out.println(parse("2017-10-23 11:40:00"));
        //System.out.println(DateFormatUtils.format(Long.parseLong("1508740204107"), "yyyy-MM-dd HH:mm:ss"));
        while (true) {
            Thread.sleep(1000);
            past1000Second();
        }
    }

}
