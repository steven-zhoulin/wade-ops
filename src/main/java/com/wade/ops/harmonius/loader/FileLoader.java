package com.wade.ops.harmonius.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileLoader extends Thread {

    private static final Log LOG = LogFactory.getLog(FileLoader.class);

    private File file;
    private File loading;
    private File history;

    public FileLoader(File file) {
        this.file = file;
    }

    @Override
    public void run() {
        long begin = System.currentTimeMillis();
        loading = new File(file.getAbsolutePath() + ".loading");
        history = new File(file.getAbsolutePath() + ".history");

        file.renameTo(loading);
        long cost = System.currentTimeMillis() - begin + 2;
        loading.renameTo(history);

        LOG.info("loading " + file.getAbsolutePath() + " cost: " + cost + " ms");
    }

}