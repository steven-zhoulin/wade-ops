package com.wade.ops.harmonius.crawler;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/08/31
 */
public class Main {

    private static Log LOG = LogFactory.getLog(Main.class);

    public static void main(String[] args) throws IOException {

        System.out.println("Main.main");
        LOG.info("-- INFO --" + timestamp());
        LOG.debug("-- DEBUG --" + timestamp());

        InputStream in = Main.class.getResourceAsStream("/hosts.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);

        List<Host> hosts = JSON.parseArray(content.trim(), Host.class);
        for (Host host : hosts) {
            System.out.println(host);
        }

        // [OK] 获取当前时间戳的10分钟归属位置。
        // [OK] 读取web,app主机的地址、账号、密码、目录位置、文件特征标识。
        // 多线程到服务器上通过FTP或SFTP爬bomc.*.dat文件下来。
        // 爬下来的文件改文件后缀名为 .dat.fin 表示已经下载完毕。
        // ...
        // 往HBase灌入数据。
        //
    }

    /**
     * 获取当前时间戳，格式: MMddHHm0
     *
     * @return
     */
    private static final String timestamp() {

        String timestamp = DateFormatUtils.format(System.currentTimeMillis(), "MMddHHmm");
        return timestamp.substring(0, 7) + "0";

    }

}
