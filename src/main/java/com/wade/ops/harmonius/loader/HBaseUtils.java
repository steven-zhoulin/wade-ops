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

    private static HTable ht_trace = null;
    private static HTable ht_trace_menu = null;
    private static HTable ht_trace_operid = null;

    static {
        try {

            connection = ConnectionFactory.createConnection(configuration);
            ht_trace = (HTable) connection.getTable(TableName.valueOf("trace"));
            ht_trace_menu = (HTable) connection.getTable(TableName.valueOf("trace_menu"));
            ht_trace_operid = (HTable) connection.getTable(TableName.valueOf("trace_operid"));

            ht_trace.setAutoFlushTo(false);
            ht_trace_menu.setAutoFlushTo(false);
            ht_trace_operid.setAutoFlushTo(false);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HBaseUtils() {}

    public static void tracePut(Put put) {
        try {
            ht_trace.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceMenuPut(Put put) {
        try {
            ht_trace_menu.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceOperidPut(Put put) {
        try {
            ht_trace_operid.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceFlushCommits() {
        try {
            ht_trace.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceMenuFlushCommits() {
        try {
            ht_trace_menu.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceOpenidFlushCommits() {
        try {
            ht_trace_operid.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void commitAndClose() {
        try {
            ht_trace.flushCommits();
            ht_trace_menu.flushCommits();
            ht_trace_operid.flushCommits();

            ht_trace.close();
            ht_trace_menu.close();
            ht_trace_operid.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
