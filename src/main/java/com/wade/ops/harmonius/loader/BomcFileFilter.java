package com.wade.ops.harmonius.loader;

import com.wade.ops.harmonius.Utils;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class BomcFileFilter implements FilenameFilter {

    private String timestamp;

    public BomcFileFilter() {
        this.timestamp = Utils.previousOneCycle();
    }

    @Override
    public boolean accept(File dir, String filename) {
        if (filename.startsWith("bomc.") && filename.endsWith(timestamp + ".dat")) {
            return true;
        } else {
            return true;
        }
    }

}
