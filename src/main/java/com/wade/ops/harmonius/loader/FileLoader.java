package com.wade.ops.harmonius.loader;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 将bomc文件解析后加载到HBase
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileLoader extends Thread {

    private static final Log LOG = LogFactory.getLog(FileLoader.class);

    private static final byte[] CF_SPAN = Bytes.toBytes("span");
    private static final byte[] FAMILY_INFO = Bytes.toBytes("info");
    private static final byte[] COL_TID = Bytes.toBytes("tid");

    private File file = null;
    private File loading = null;
    private StopWatch stopWatch = new StopWatch();

    FileLoader(File file) {

        // 文件加载任务一开始首先把文件名改掉，防止重复加载。
        this.file = file;
        this.loading = new File(file.getAbsolutePath() + ".loading");
        if (file.renameTo(loading)) {
            LOG.info("begin loading " + file.getName());
        } else {
            LOG.error(file.getName() + ": .dat -> .dat.loading failure! ");
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void run() {

        stopWatch.start();
        load();
        stopWatch.split();

        // 将处理完的文件改成历史文件
        LOG.info(String.format("loaded %-70s cost: %5d ms", file.getAbsolutePath(), stopWatch.getSplitTime()));
        this.loading = new File(file.getAbsolutePath() + ".loading");
        File loaded = new File(file.getAbsolutePath() + ".loaded");
        this.loading.renameTo(loaded);

    }

    @SuppressWarnings("unchecked")
    private void load() {

        FileInputStream fis;
        ObjectInputStream ois = null;

        try {

            fis = new FileInputStream(this.loading);
            ois = new ObjectInputStream(fis);

            while (true) {

                HashMap<String, Object> span;

                try {
                    span = (HashMap<String, Object>) ois.readObject();
                } catch (EOFException e) {
                    break; // 读到文件末尾, 正常!
                }

                if (null == span) {
                    continue;
                }

                String traceid = (String) span.get("traceid");
                if (null != traceid) {
                    loadTrace(span);
                    loadTraceMenu(span);
                    loadTraceOperid(span);
                    loadTraceSn(span);
                    loadTraceService(span);
                }

            }

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {

            HBaseUtils.traceFlushCommits();
            HBaseUtils.traceMenuFlushCommits();
            HBaseUtils.traceOpenidFlushCommits();
            HBaseUtils.traceSnFlushCommits();
            HBaseUtils.traceServiceFlushCommits();

            try {
                if (null != ois) {
                    ois.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * put 'trace', 'web-74138c1248e44ca5b1ac7991b7635711', 'span:browser|74138c1248e44ca5b1ac7991b7635711', byte[]
     *
     * @param span
     */
    private static void loadTrace(HashMap<String, Object> span) {

        String traceid = (String) span.get("traceid");
        String probetype = (String) span.get("probetype");
        String id = (String) span.get("id");

        String rowkey = traceid;
        String colname = probetype + "|" + id;

        byte[] data = SerializationUtils.serialize(span);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(CF_SPAN, Bytes.toBytes(colname), data);
        HBaseUtils.tracePut(put);

    }

    /**
     * put 'trace_menu', 'CRM0001^201710201430', 'tid:web-74138c1248e44ca5b1ac7991b7635711', ''
     *
     * @param span
     */
    private static void loadTraceMenu(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

        if (!"web".equals(probetype)) {
            return;
        }

        String traceid = (String) span.get("traceid");
        String menuid = (String) span.get("menuid");
        String starttime = (String) span.get("starttime");
        String rowkey = menuid + "^" + starttime;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
        HBaseUtils.traceMenuPut(put);

    }

    /**
     * put 'trace_operid', 'SUPERUSR^201710201430', 'info:tid', 'web-74138c1248e44ca5b1ac7991b7635711'
     *
     * @param span
     */
    private static void loadTraceOperid(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

        if (!"app".equals(probetype)) {
            return;
        }

        String traceid = (String) span.get("traceid");
        String starttime = (String) span.get("starttime");
        String operid = (String) span.get("operid");
        String rowkey = operid + "^" + starttime;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
        HBaseUtils.traceOperidPut(put);

    }

    /**
     * put 'trace_sn', '13007318123^201710201430', 'info:tid', 'web-74138c1248e44ca5b1ac7991b7635711'
     *
     * @param span
     */
    @SuppressWarnings("unchecked")
    private static void loadTraceSn(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

        // SN只会从APP的入参里取，记得配置WD_APPLOG_CONF表
        if (!"app".equals(probetype)) {
            return;
        }

        Map<String, String> ext = (Map<String, String>) span.get("ext");
        if (null == ext || 0 == ext.size()) {
            return;
        }

        String sn = ext.get("SERIAL_NUMBER");
        if (null == sn) {
            return;
        }

        String traceid = (String) span.get("traceid");
        String starttime = (String) span.get("starttime");
        String rowkey = sn + "^" + starttime;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
        HBaseUtils.traceSnPut(put);

    }

    /**
     * put 'trace_service', 'SVCNAME1^201710201430', 'info:tid', 'tid:web-74138c1248e44ca5b1ac7991b7635711'
     *
     * @param span
     */
    private static void loadTraceService(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

        if (!"service".equals(probetype)) {
            return;
        }

        String servicename = (String) span.get("servicename");
        if (null == servicename) {
            return;
        }

        String traceid = (String) span.get("traceid");
        String starttime = (String) span.get("starttime");
        String rowkey = servicename + "^" + starttime;
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
        HBaseUtils.traceServicePut(put);

    }

}