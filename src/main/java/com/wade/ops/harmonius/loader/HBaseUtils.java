package com.wade.ops.harmonius.loader;

import com.wade.ops.harmonius.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

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
    private static HTable ht_trace_sn = null;
    private static HTable ht_trace_service = null;
    private static HTable ht_service_map = null;
    private static HTable ht_sink_service_relat = null;

    static {

        try {

            connection = ConnectionFactory.createConnection(configuration);
            ht_trace = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE));
            ht_trace_menu = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_MENU));
            ht_trace_operid = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_OPERID));
            ht_trace_sn = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_SN));
            ht_trace_service = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_SERVICE));
            ht_service_map = (HTable) connection.getTable(TableName.valueOf(Constants.HT_SERVICE_MAP));
            ht_sink_service_relat = (HTable) connection.getTable(TableName.valueOf(Constants.HT_SINK_SERVICE_RELAT));

            ht_trace.setAutoFlushTo(false);
            ht_trace_menu.setAutoFlushTo(false);
            ht_trace_operid.setAutoFlushTo(false);
            ht_trace_sn.setAutoFlushTo(false);
            ht_trace_service.setAutoFlushTo(false);
            ht_service_map.setAutoFlushTo(false);
            ht_sink_service_relat.setAutoFlushTo(false);

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

    public static void traceSnPut(Put put) {
        try {
            ht_trace_sn.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceServicePut(Put put) {
        try {
            ht_trace_service.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void serviceMapPut(Put put) {
        try {
            ht_service_map.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sinkServiceRelatIncrement(Increment increment) {
        try {
            HBaseUtils.ht_sink_service_relat.increment(increment);
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

    public static void traceSnFlushCommits() {
        try {
            ht_trace_sn.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void traceServiceFlushCommits() {
        try {
            ht_trace_service.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void serviceMapFlushCommits() {
        try {
            ht_service_map.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sinkServiceRelatFlushCommits() {
        try {
            ht_sink_service_relat.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void commitAndClose() {
        try {

            ht_trace.flushCommits();
            ht_trace_menu.flushCommits();
            ht_trace_operid.flushCommits();
            ht_trace_sn.flushCommits();
            ht_trace_service.flushCommits();
            ht_service_map.flushCommits();
            ht_sink_service_relat.flushCommits();

            ht_trace.close();
            ht_trace_menu.close();
            ht_trace_operid.close();
            ht_trace_sn.close();
            ht_trace_service.close();
            ht_service_map.close();
            ht_sink_service_relat.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
