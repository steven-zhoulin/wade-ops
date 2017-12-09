package com.wade.ops.harmonius;

import java.util.HashMap;
import java.util.List;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/05
 */
public class Example {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "D:/devsoft/hadoop-common-2.2.0-bin-master");
        /*
        List<String> tids = OpsHBaseAPI.getInstance().selectByMenuId("BIL1101","2017-12-06 00:00:00","2017-12-08 00:00:00");
        for (String tid : tids) {
            System.out.println(tid);
        }*/


        List<HashMap<String, Object>> list = OpsHBaseAPI.getInstance().selectByTraceId("web-6d9ef18e0fff4b21ae7f056ac7597bdb");
        for (HashMap<String, Object> o : list) {
            System.out.println(o);
        }
    }

}
