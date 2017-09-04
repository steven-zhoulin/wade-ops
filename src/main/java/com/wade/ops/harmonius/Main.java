package com.wade.ops.harmonius;

import com.alibaba.fastjson.JSON;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.wade.ops.harmonius.crawler.FileCrawlerScheduler;
import com.wade.ops.harmonius.crawler.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/08/31
 */
public class Main {

    private static final Log LOG = LogFactory.getLog(Main.class);

    public static Config config = null;

    private static ExecutorService EXECUTE_SERVICE_LOADING;

    /**
     * 加载配置文件
     *
     * @return
     * @throws IOException
     */
    private static final Config loadConfig() throws IOException {

        LOG.info("loading configuration config.json...");
        InputStream in = Main.class.getResourceAsStream("/config.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);
        return JSON.parseObject(content.trim(), Config.class);

    }

    public static void main(String[] args) throws IOException, SftpException, JSchException {

        config = loadConfig();

        LOG.info("crawler pool size: " + config.getCrawlerPoolsize());
        LOG.info("loading pool size: " + config.getLoadingPoolsize());

        EXECUTE_SERVICE_LOADING = Executors.newFixedThreadPool(config.getLoadingPoolsize());

        FileCrawlerScheduler fileCrawlerScheduler = new FileCrawlerScheduler(config.getCrawlerPoolsize());
        fileCrawlerScheduler.doWork();

        // [OK] 获取当前时间戳的10分钟归属位置。
        // [OK] 读取web,app主机的地址、账号、密码、目录位置、文件特征标识。
        // 多线程到服务器上通过FTP或SFTP爬bomc.*.dat文件下来。
        // 支持一边爬文件下来，一边录入HBase
        // 以时间戳建目录: .dat.crawling -> .dat -> .dat.loading -> .dat.history
        // 按时间戳打包压缩处理，定时删除。

        // 仅用于测试
        fileCrawlerScheduler.shutdown();
        EXECUTE_SERVICE_LOADING.shutdown();
    }
}
