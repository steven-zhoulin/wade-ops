package com.wade.ops.harmonius;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/05
 */
public class Example {
    public static void main(String[] args) throws IOException {
        System.out.println("----------------------------------");
        FileUtils.forceMkdir(new File("D:\\bomc"));
        System.out.println("----------------------------------");
    }
}
