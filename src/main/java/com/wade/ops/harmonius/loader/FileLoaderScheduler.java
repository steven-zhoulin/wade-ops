package com.wade.ops.harmonius.loader;

import com.wade.ops.harmonius.CrawlState;
import com.wade.ops.harmonius.Main;
import com.wade.ops.harmonius.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileLoaderScheduler {

    private static final Log LOG = LogFactory.getLog(FileLoaderScheduler.class);

    private static ExecutorService EXECUTE_SERVICE_LOADING;

    public FileLoaderScheduler(int loaderPoolsize) {
        EXECUTE_SERVICE_LOADING = Executors.newFixedThreadPool(loaderPoolsize);
    }

    public void doWork() {

        String directory = Utils.getBomcCurrDirectory();
        String timestamp = Utils.previousOneCycle();
        File bomcDir = new File(directory);

        while (true) {

            CrawlState state = Main.STATES.get(timestamp);
            if (CrawlState.BEGIN == state) {
                LOG.info("loader work begin, previousOneCycle: " + timestamp + " " + directory);

                for (File file : bomcDir.listFiles(new BomcFileFilter())) {
                    FileLoader fileLoader = new FileLoader(file);
                    EXECUTE_SERVICE_LOADING.execute(fileLoader);
                }

            } else if (CrawlState.END == state) {

                for (File file : bomcDir.listFiles(new BomcFileFilter())) {
                    FileLoader fileLoader = new FileLoader(file);
                    EXECUTE_SERVICE_LOADING.execute(fileLoader);
                }

                timestamp = Utils.previousOneCycle();

            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void shutdown() {
        EXECUTE_SERVICE_LOADING.shutdown();
    }
}