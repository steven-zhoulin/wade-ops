package com.wade.ops.harmonius;

import com.wade.ops.harmonius.loader.HBaseUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class Test {

    private static final Log LOG = LogFactory.getLog(Test.class);
    private static final byte[] FAMILY_INFO = Bytes.toBytes("info");
    private static final byte[] NAMES = Bytes.toBytes("name");
    private static final byte[] AGE = Bytes.toBytes("age");
    private static final byte[] SALARY = Bytes.toBytes("salary");

    public Test() {

    }

    public void run() {

        long count = 0L;
        while (true) {

            String rowkey = UUID.randomUUID().toString();

            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(FAMILY_INFO, NAMES, Bytes.toBytes("\"wangwu\""));
            put.addColumn(FAMILY_INFO, AGE, Bytes.toBytes("28"));
            put.addColumn(FAMILY_INFO, SALARY, Bytes.toBytes("12345"));

            HBaseUtils.put(put);
            count++;

            if (0 == count % 1000) {
                HBaseUtils.flushCommits();
                LOG.info(String.format("共计插入: %-10d条记录!", count));
            }
        }

    }

}