package com.wade.ops.harmonius;

import com.wade.ops.util.TripleDES;

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
        System.out.println("----------------------------------");
        System.out.println(TripleDES.decrypt("tsY0WGlMMac+6JKhtwDcZQ=="));
    }
}
