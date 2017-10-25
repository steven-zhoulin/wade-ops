package com.wade.ops.util;

import com.wade.ops.harmonius.OpsLoadMain;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateUtil {

    /**
     * 返回当前时间所对应的工作目录
     *
     * @return
     */
    public static String currentBomcDirectory() {
        String bomcBaseDirectory = OpsLoadMain.config.getBomcBaseDirectory();
        return bomcBaseDirectory + File.separatorChar + previousOneCycle();
    }

    /**
     * 获取即将被删除的目录
     *
     * @return
     */
    public static String beRemovedDirectory() {
        String bomcBaseDirectory = OpsLoadMain.config.getBomcBaseDirectory();
        int backupIndex = OpsLoadMain.config.getBackupIndex();
        return bomcBaseDirectory + File.separatorChar + timestamp(0 - backupIndex);
    }

    /**
     * 获取上一个周期的时间戳，格式: MMddHHm0
     *
     * @return
     */
    public static final String previousOneCycle() {

        String timestamp = OpsLoadMain.config.getTimestamp();

        if (null != timestamp) {
            return timestamp;
        } else {
            timestamp = timestamp(-1);
            return timestamp;
        }
    }

    /**
     * 根据周期参数获取时间戳，0: 当前时间戳；-1: 上一个周期时间戳；1：下一个周期时间戳
     *
     * @param cycle
     * @return
     */
    public static final String timestamp(int cycle) {
        String timestamp = DateFormatUtils.format(System.currentTimeMillis() + (1000 * 600 * cycle), "MMddHHmm");
        return timestamp.substring(0, 7) + "0";
    }

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

    public static void main(String[] args) throws Exception {
        // System.out.println(parse("2017-10-23 11:39:59"));
        // System.out.println(parse("2017-10-23 11:40:00"));
        // System.out.println(DateFormatUtils.format(Long.parseLong("1508740204107"), "yyyy-MM-dd HH:mm:ss"));
    }

}
