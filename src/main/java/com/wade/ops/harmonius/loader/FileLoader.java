package com.wade.ops.harmonius.loader;

import org.apache.commons.lang.SerializationUtils;
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
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class FileLoader extends Thread {

    private static final Log LOG = LogFactory.getLog(FileLoader.class);

    private static final byte[] NULL = new byte[0];
    private static final byte[] CF_SPAN = Bytes.toBytes("span");
    private static final byte[] CF_TID = Bytes.toBytes("tid");

    private static final byte[] FAMILY_INFO = Bytes.toBytes("info");
    private static final byte[] COL_TID = Bytes.toBytes("tid");

    private static final byte[] COL_TRACEID = Bytes.toBytes("traceid");
    private static final byte[] COL_ID = Bytes.toBytes("id");
    private static final byte[] COL_PARENTID = Bytes.toBytes("parentid");
    private static final byte[] COL_STARTTIME = Bytes.toBytes("starttime");
    private static final byte[] COL_ENDTIME = Bytes.toBytes("endtime");
    private static final byte[] COL_COSTTIME = Bytes.toBytes("costtime");
    private static final byte[] COL_PROBETYPE = Bytes.toBytes("probetype");
    private static final byte[] COL_BIZID = Bytes.toBytes("bizid");
    private static final byte[] COL_OPERID = Bytes.toBytes("operid");
    private static final byte[] COL_SESSIONID = Bytes.toBytes("sessionid");
    private static final byte[] COL_URL = Bytes.toBytes("url");
    private static final byte[] COL_CLIENTIP = Bytes.toBytes("clientip");
    private static final byte[] COL_SERVERNAME = Bytes.toBytes("servername");
    private static final byte[] COL_MENUID = Bytes.toBytes("menuid");
    private static final byte[] COL_STATUSCODE = Bytes.toBytes("statuscode");
    private static final byte[] COL_IP = Bytes.toBytes("ip");
    private static final byte[] COL_SERVICENAME = Bytes.toBytes("servicename");
    private static final byte[] COL_MAINSERVICE = Bytes.toBytes("mainservice");
    private static final byte[] COL_DATASOURCE = Bytes.toBytes("datasource");
    private static final byte[] COL_SQLNAME = Bytes.toBytes("sqlname");
    private static final byte[] COL_SQL = Bytes.toBytes("sql");

    private File file = null;
    private File loading = null;

    public FileLoader(File file) {

        // 文件加载任务一开始首先把文件名改掉，防止重复加载。
        this.file = file;
        this.loading = new File(file.getAbsolutePath() + ".loading");
        file.renameTo(loading);
        LOG.info("begin loading " + file.getName());
    }

    @Override
    public void run() {
        long begin = System.currentTimeMillis();
        load();
        long cost = System.currentTimeMillis() - begin;

        // 将处理完的文件改成历史文件
        LOG.info(String.format("loaded %-70s cost: %5d ms", file.getAbsolutePath(), cost));
        this.loading = new File(file.getAbsolutePath() + ".loading");
        File loaded = new File(file.getAbsolutePath() + ".loaded");
        this.loading.renameTo(loaded);
    }

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

                /*
                BaseData baseData = loadBaseTraceData(span);
                String probetype = baseData.getProbetype();

                switch (probetype) {
                    case "browser":
                        loadBrowserTraceData(span, baseData);
                        break;
                    case "web":
                        loadWebTraceData(span, baseData);
                        break;
                    case "app":
                        loadAppTraceData(span, baseData);
                        break;
                    case "service":
                        loadServiceTraceData(span, baseData);
                        break;
                    case "dao":
                        loadDaoTraceData(span, baseData);
                        break;
                }
                */
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

        if (probetype.equals("web")) {
            String traceid = (String) span.get("traceid");
            String menuid = (String) span.get("menuid");
            String starttime = (String) span.get("starttime");

            String rowkey = menuid + "^" + starttime;
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
            HBaseUtils.traceMenuPut(put);
        }

    }

    /**
     * put 'trace_operid', 'SUPERUSR^201710201430', 'info:tid', 'web-74138c1248e44ca5b1ac7991b7635711'
     *
     * @param span
     */
    private static void loadTraceOperid(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

        if (probetype.equals("app")) {

            String traceid = (String) span.get("traceid");
            String starttime = (String) span.get("starttime");
            String operid = (String) span.get("operid");
            String rowkey = operid + "^" + starttime;
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(FAMILY_INFO, COL_TID, Bytes.toBytes(traceid));
            HBaseUtils.traceOperidPut(put);
        }
    }

    /**
     * put 'trace_sn', '13007318123^201710201430', 'info:tid', 'web-74138c1248e44ca5b1ac7991b7635711'
     *
     * @param span
     */
    private static void loadTraceSn(HashMap<String, Object> span) {

        String probetype = (String) span.get("probetype");

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

    private BaseData loadBaseTraceData(HashMap<String, Object> map) throws IOException {

        String probetype = (String) map.get("probetype");
        String id = (String) map.get("id");
        String parentid = (String) map.get("parentid");
        String traceid = (String) map.get("traceid");
        String bizid = (String) map.get("bizid");
        String operid = (String) map.get("operid");
        String starttime = (String) map.get("starttime");
        String endtime = (String) map.get("endtime");
        String costtime = (String) map.get("costtime");

        BaseData baseData = new BaseData();
        baseData.setProbetype(probetype);
        baseData.setId(id);
        baseData.setParentid(parentid);
        baseData.setTraceid(traceid);
        baseData.setBizid(bizid);
        baseData.setOperid(operid);
        baseData.setStarttime(starttime);
        baseData.setEndtime(endtime);
        baseData.setCosttime(costtime);

        return baseData;
    }

    private static void loadBrowserTraceData(HashMap<String, Object> map, BaseData baseData) throws IOException {

        // 特殊参数
        String statuscode = (String) map.get("statuscode");

        String rowkey = baseData.getTraceid() + "|" + baseData.getId();

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_PROBETYPE, Bytes.toBytes(baseData.getProbetype()));
        put.addColumn(FAMILY_INFO, COL_ID, Bytes.toBytes(baseData.getId()));
        put.addColumn(FAMILY_INFO, COL_PARENTID, Bytes.toBytes(baseData.getParentid()));
        put.addColumn(FAMILY_INFO, COL_TRACEID, Bytes.toBytes(baseData.getTraceid()));
        put.addColumn(FAMILY_INFO, COL_STARTTIME, Bytes.toBytes(baseData.getStarttime()));
        put.addColumn(FAMILY_INFO, COL_ENDTIME, Bytes.toBytes(baseData.getEndtime()));
        put.addColumn(FAMILY_INFO, COL_COSTTIME, Bytes.toBytes(baseData.getCosttime()));

        put.addColumn(FAMILY_INFO, COL_STATUSCODE, Bytes.toBytes(statuscode));

        //HBaseUtils.put(put);
        //HBaseUtils.flushCommits();

    }

    @SuppressWarnings("unused")
    private static void loadWebTraceData(HashMap<String, Object> map, BaseData baseData) throws IOException {

        // 特殊参数
        String sessionid = (String) map.get("sessionid");
        String clientip = (String) map.get("clientip");
        String ip = (String) map.get("ip");
        String servername = (String) map.get("servername");
        String url = (String) map.get("url");
        String menuid = (String) map.get("menuid");

        String rowkey = baseData.getTraceid() + "|" + baseData.getId();
        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(FAMILY_INFO, COL_PROBETYPE, Bytes.toBytes(baseData.getProbetype()));

        put.addColumn(FAMILY_INFO, COL_ID, Bytes.toBytes(baseData.getId()));
        put.addColumn(FAMILY_INFO, COL_PARENTID, Bytes.toBytes(baseData.getParentid()));
        put.addColumn(FAMILY_INFO, COL_TRACEID, Bytes.toBytes(baseData.getTraceid()));
        put.addColumn(FAMILY_INFO, COL_BIZID, Bytes.toBytes(baseData.getBizid()));
        put.addColumn(FAMILY_INFO, COL_OPERID, Bytes.toBytes(baseData.getOperid()));
        put.addColumn(FAMILY_INFO, COL_STARTTIME, Bytes.toBytes(baseData.getStarttime()));
        put.addColumn(FAMILY_INFO, COL_ENDTIME, Bytes.toBytes(baseData.getEndtime()));
        put.addColumn(FAMILY_INFO, COL_COSTTIME, Bytes.toBytes(baseData.getCosttime()));

        put.addColumn(FAMILY_INFO, COL_SERVERNAME, Bytes.toBytes(servername));
        put.addColumn(FAMILY_INFO, COL_SESSIONID, Bytes.toBytes(sessionid));
        put.addColumn(FAMILY_INFO, COL_CLIENTIP, Bytes.toBytes(clientip));
        put.addColumn(FAMILY_INFO, COL_IP, Bytes.toBytes(ip));
        put.addColumn(FAMILY_INFO, COL_URL, Bytes.toBytes(url));
        put.addColumn(FAMILY_INFO, COL_MENUID, Bytes.toBytes(menuid));

       // HBaseUtils.put(put);
        //HBaseUtils.flushCommits();

    }

    private static void loadAppTraceData(HashMap<String, Object> map, BaseData baseData) throws IOException {

        // 特殊参数
        String servername = (String) map.get("servername");
        String ip = (String) map.get("ip");

        String rowkey = baseData.getTraceid() + "|" + baseData.getId();
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_PROBETYPE, Bytes.toBytes(baseData.getProbetype()));
        put.addColumn(FAMILY_INFO, COL_ID, Bytes.toBytes(baseData.getId()));
        put.addColumn(FAMILY_INFO, COL_PARENTID, Bytes.toBytes(baseData.getParentid()));
        put.addColumn(FAMILY_INFO, COL_TRACEID, Bytes.toBytes(baseData.getTraceid()));
        put.addColumn(FAMILY_INFO, COL_OPERID, Bytes.toBytes(baseData.getOperid()));
        put.addColumn(FAMILY_INFO, COL_STARTTIME, Bytes.toBytes(baseData.getStarttime()));
        put.addColumn(FAMILY_INFO, COL_ENDTIME, Bytes.toBytes(baseData.getEndtime()));
        put.addColumn(FAMILY_INFO, COL_COSTTIME, Bytes.toBytes(baseData.getCosttime()));

        put.addColumn(FAMILY_INFO, COL_SERVERNAME, Bytes.toBytes(servername));
        put.addColumn(FAMILY_INFO, COL_IP, Bytes.toBytes(ip));

        //HBaseUtils.put(put);
        //HBaseUtils.flushCommits();

    }

    private static void loadServiceTraceData(HashMap<String, Object> map, BaseData baseData) throws IOException {

        // 特殊参数
        String servicename = (String) map.get("servicename");
        String mainservice = Boolean.toString((boolean) map.get("mainservice"));

        String rowkey = baseData.getTraceid() + "|" + baseData.getId();
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_PROBETYPE, Bytes.toBytes(baseData.getProbetype()));
        put.addColumn(FAMILY_INFO, COL_ID, Bytes.toBytes(baseData.getId()));
        put.addColumn(FAMILY_INFO, COL_PARENTID, Bytes.toBytes(baseData.getParentid()));
        put.addColumn(FAMILY_INFO, COL_TRACEID, Bytes.toBytes(baseData.getTraceid()));
        put.addColumn(FAMILY_INFO, COL_OPERID, Bytes.toBytes(baseData.getOperid()));
        put.addColumn(FAMILY_INFO, COL_STARTTIME, Bytes.toBytes(baseData.getStarttime()));
        put.addColumn(FAMILY_INFO, COL_ENDTIME, Bytes.toBytes(baseData.getEndtime()));
        put.addColumn(FAMILY_INFO, COL_COSTTIME, Bytes.toBytes(baseData.getCosttime()));

        put.addColumn(FAMILY_INFO, COL_SERVICENAME, Bytes.toBytes(servicename));
        put.addColumn(FAMILY_INFO, COL_MAINSERVICE, Bytes.toBytes(mainservice));

        //HBaseUtils.put(put);
        //HBaseUtils.flushCommits();

    }

    private static void loadDaoTraceData(HashMap<String, Object> map, BaseData baseData) throws IOException {

        // 特殊参数
        //String datasource = (String) map.get("datasource");
        String sqlname = (String) map.get("sqlname");
        String sql = (String) map.get("sql");

        String rowkey = baseData.getTraceid() + "|" + baseData.getId();
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(FAMILY_INFO, COL_PROBETYPE, Bytes.toBytes(baseData.getProbetype()));
        put.addColumn(FAMILY_INFO, COL_ID, Bytes.toBytes(baseData.getId()));
        put.addColumn(FAMILY_INFO, COL_PARENTID, Bytes.toBytes(baseData.getParentid()));
        put.addColumn(FAMILY_INFO, COL_TRACEID, Bytes.toBytes(baseData.getTraceid()));
        put.addColumn(FAMILY_INFO, COL_OPERID, Bytes.toBytes(baseData.getOperid()));
        put.addColumn(FAMILY_INFO, COL_STARTTIME, Bytes.toBytes(baseData.getStarttime()));
        put.addColumn(FAMILY_INFO, COL_ENDTIME, Bytes.toBytes(baseData.getEndtime()));
        put.addColumn(FAMILY_INFO, COL_COSTTIME, Bytes.toBytes(baseData.getCosttime()));

        //put.addColumn(FAMILY_INFO, COL_DATASOURCE, Bytes.toBytes(datasource));
        put.addColumn(FAMILY_INFO, COL_SQLNAME, Bytes.toBytes(sqlname));
        put.addColumn(FAMILY_INFO, COL_SQL, Bytes.toBytes(sql));

        //HBaseUtils.put(put);
        //HBaseUtils.flushCommits();
    }

}