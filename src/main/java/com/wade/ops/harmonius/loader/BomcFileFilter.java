package com.wade.ops.harmonius.loader;

import com.wade.ops.util.DateUtil;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: bomc日志文件过滤器
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class BomcFileFilter implements FilenameFilter {

    private String timestamp;

    public BomcFileFilter() {
        this.timestamp = DateUtil.previousOneCycle();
    }

    @Override
    public boolean accept(File dir, String filename) {
        return filename.startsWith("bomc.") && filename.endsWith(timestamp + ".dat");

    }

}
