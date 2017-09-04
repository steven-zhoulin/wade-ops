package com.wade.ops.harmonius.crawler;

import com.wade.ops.harmonius.CrawlState;
import com.wade.ops.harmonius.Main;
import com.wade.ops.harmonius.Utils;
import com.wade.ops.harmonius.crawler.config.Host;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 文件爬取任务调度
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileCrawlerScheduler {

    private static final Log LOG = LogFactory.getLog(FileCrawlerScheduler.class);
    private ExecutorService executorService;
    private int crawlerPoolSize;
    private String timestamp = "";

    public FileCrawlerScheduler(int crawlerPoolSize) {
        this.crawlerPoolSize = crawlerPoolSize;
    }

    public void doWork() throws Exception {

        while (true) {

            Thread.sleep(1000 * 10);

            // 只有在时间戳发生跳变的时候才开始新一轮的文件爬取工作
            if (Utils.previousOneCycle().equals(timestamp)) {
                continue;
            } else {
                timestamp = Utils.previousOneCycle();
                executorService = Executors.newFixedThreadPool(crawlerPoolSize);
            }

            try {

                LOG.info("crawler work begin, previousOneCycle: " + timestamp);
                Main.STATES.put(timestamp, CrawlState.BEGIN);
                Main.STATES.remove(Utils.timestamp(-10));

                String directory = Utils.getBomcCurrDirectory();
                FileUtils.forceMkdir(new File(directory));

                List<Host> hosts = Main.config.getHosts();
                for (Host host : hosts) {
                    FileCrawler fileCrawler = new FileCrawler(host, timestamp, directory);
                    executorService.execute(fileCrawler);
                }

                executorService.shutdown(); // 执行以前提交的任务，不再接受新任务
                while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    continue;
                }

                // 通知加载线程，本周期内crawl的工作做完了。
                Main.STATES.put(timestamp, CrawlState.END);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public void shutdown() {
        executorService.shutdown();
    }
}
