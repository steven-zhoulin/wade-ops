package com.wade.ops.harmonius.crawler;

import com.alibaba.fastjson.JSON;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.wade.ops.harmonius.crawler.config.Config;
import com.wade.ops.harmonius.crawler.config.Host;
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

    public static void main(String[] args) throws IOException, SftpException, JSchException {

        String timestamp = timestamp();
        LOG.info("crawler work begin, timestamp: " + timestamp);

        Config config = loadConfig();
        List<Host> hosts = config.getHosts();
        BomcCrawler crawler = new BomcCrawler();
        for (Host host : hosts) {
            long begin = System.currentTimeMillis();

            LOG.info("");
            LOG.info("Resource from " + host.getUser() + "@" + host.getHost() + ":" + host.getPath());

            int count = crawler.crawlFilesBySftp(config, host, "", "D:/bomc/");

            LOG.info("---------------------------------------------");
            LOG.info("count: " + count + ", costtime: " + (System.currentTimeMillis() - begin) + "ms");
        }

        // [OK] 获取当前时间戳的10分钟归属位置。
        // [OK] 读取web,app主机的地址、账号、密码、目录位置、文件特征标识。
        // 多线程到服务器上通过FTP或SFTP爬bomc.*.dat文件下来。
        // 爬下来的文件改文件后缀名为 .dat.fin 表示已经下载完毕。
        // json格式配置文件要支持参数替换
        // ...
        // 往HBase灌入数据。
        //
    }

    /**
     * 加载配置文件
     *
     * @return
     * @throws IOException
     */
    private static final Config loadConfig() throws IOException {

        InputStream in = Main.class.getResourceAsStream("/config.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);
        return JSON.parseObject(content.trim(), Config.class);

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
