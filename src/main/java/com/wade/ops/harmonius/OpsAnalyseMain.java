package com.wade.ops.harmonius;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 分析启动类
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class OpsAnalyseMain implements Constants {

    public static void main(String[] args) throws Exception {

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.boot();

        OpsAnalyse opsAnalyse = new OpsAnalyse();
        opsAnalyse.start();

    }

}