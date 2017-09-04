package com.wade.ops.harmonius;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.File;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public final class Utils {

    /**
     * 返回当前时间所对应的工作目录
     *
     * @return
     */
    public static String getBomcCurrDirectory() {
        String bomcBaseDirectory = Main.config.getBomcBaseDirectory();
        String bomcCurrDirectory = bomcBaseDirectory + File.separatorChar + previousOneCycle();
        return bomcCurrDirectory;
    }

    /**
     * 获取上一个周期的时间戳，格式: MMddHHm0
     *
     * @return
     */
    public static final String previousOneCycle() {
        String timestamp = timestamp(-1);
        //return previousOneCycle;
        return "11111220";
    }

    /**
     * 获取上两个周期的时间戳，格式: MMddHHm0
     *
     * @return
     */
    public static final String previousTwoCycle() {
        String timestamp = timestamp(-2);
        return timestamp;
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
}
