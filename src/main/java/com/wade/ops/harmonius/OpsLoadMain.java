package com.wade.ops.harmonius;

import com.alibaba.fastjson.JSON;
import com.wade.ops.harmonius.crawler.FileCrawlerScheduler;
import com.wade.ops.config.Config;
import com.wade.ops.harmonius.loader.FileLoaderScheduler;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 数据加载主启动类
 * @auth: steven.zhou
 * @date: 2017/08/31
 */
public class OpsLoadMain {

    public static Config config = null;
    public static final Map<String, CrawlState> STATES = new HashMap<>();

    /**
     * 加载配置文件
     *
     * @throws IOException
     */
    static Config loadConfig() throws IOException {

        System.out.println("loading configuration config.json...");
        InputStream in = OpsLoadMain.class.getResourceAsStream("/config.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);
        return JSON.parseObject(content.trim(), Config.class);

    }

    /**
     * 以时间戳建目录: .dat.crawling -> .dat -> .dat.loading -> .dat.loaded
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.boot();

        config = loadConfig();

        System.out.println("crawler pool size: " + config.getCrawlerPoolsize());
        System.out.println("loading pool size: " + config.getLoadingPoolsize());
        System.out.println("backupIndex: " + config.getBackupIndex());
        System.out.println("timestamp: " + config.getTimestamp());

        FileCrawlerScheduler fileCrawlerScheduler = new FileCrawlerScheduler(config.getCrawlerPoolsize());
        fileCrawlerScheduler.start();

        FileLoaderScheduler fileLoaderScheduler = new FileLoaderScheduler(config.getLoadingPoolsize());
        fileLoaderScheduler.start();

    }
}
