package com.wade.ops.harmonius.loader;

import com.wade.ops.harmonius.CrawlState;
import com.wade.ops.harmonius.OpsLoadMain;
import com.wade.ops.harmonius.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileLoaderScheduler extends Thread {

    private static final Log LOG = LogFactory.getLog(FileLoaderScheduler.class);

    private static ExecutorService EXECUTE_SERVICE_LOADING;

    public FileLoaderScheduler(int loaderPoolsize) {
        EXECUTE_SERVICE_LOADING = Executors.newFixedThreadPool(loaderPoolsize);
    }

    public void run() {

        String timestamp = Utils.previousOneCycle();

        while (true) {

            String directory = Utils.getBomcCurrDirectory();
            File bomcDir = new File(directory);

            CrawlState state = OpsLoadMain.STATES.get(timestamp);
            LOG.info("timestamp: " + timestamp + ", state: " + state);
            File[] files = bomcDir.listFiles(new BomcFileFilter());
            if (CrawlState.BEGIN == state) {

                if (null != files) {
                    for (File file : files) {
                        FileLoader fileLoader = new FileLoader(file);
                        EXECUTE_SERVICE_LOADING.execute(fileLoader);
                    }
                }

            } else if (CrawlState.END == state) {

                if (null != files) {
                    for (File file : files) {
                        FileLoader fileLoader = new FileLoader(file);
                        EXECUTE_SERVICE_LOADING.execute(fileLoader);
                    }
                }

                timestamp = Utils.previousOneCycle();

            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            File beRemovedDirectory = new File(Utils.beRemovedDirectory());
            if (beRemovedDirectory.exists()) {
                try {
                    FileUtils.deleteDirectory(beRemovedDirectory);
                    LOG.info("删除历史目录: " + beRemovedDirectory);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public void shutdown() {
        EXECUTE_SERVICE_LOADING.shutdown();
    }
}