package com.wade.ops.harmonius.crawler;

import com.jcraft.jsch.JSchException;
import com.wade.ops.harmonius.Main;
import com.wade.ops.harmonius.Utils;
import com.wade.ops.harmonius.crawler.config.Host;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/09/04
 */
public class FileCrawlerScheduler {

    private static final Log LOG = LogFactory.getLog(FileCrawlerScheduler.class);

    private static ExecutorService EXECUTE_SERVICE_CRAWLER;

    public FileCrawlerScheduler(int crawlerPoolsize) {
        EXECUTE_SERVICE_CRAWLER = Executors.newFixedThreadPool(crawlerPoolsize);
    }

    public void doWork() throws JSchException {

        String timestamp = Utils.timestamp();
        LOG.info("crawler work begin, timestamp: " + timestamp);

        List<Host> hosts = Main.config.getHosts();
        for (Host host : hosts) {
            FileCrawler fileCrawler = new FileCrawler(host, timestamp, "D:/bomc/");
            EXECUTE_SERVICE_CRAWLER.execute(fileCrawler);
        }

    }

    public void shutdown() {
        EXECUTE_SERVICE_CRAWLER.shutdown();
    }
}
