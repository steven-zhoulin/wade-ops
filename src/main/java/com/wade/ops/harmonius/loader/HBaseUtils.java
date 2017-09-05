package com.wade.ops.harmonius.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/05
 */
public class HBaseUtils {

    private static final Configuration configuration = HBaseConfiguration.create();
    private static Connection connection = null;
    private static HTable table = null;

    static {
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = (HTable) connection.getTable(TableName.valueOf("bomc"));
            table.setAutoFlushTo(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HBaseUtils() {}

    public static void put(Put put) {
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void flushCommits() {
        try {
            table.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void commitAndClose() {
        try {
            table.flushCommits();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
