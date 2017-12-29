package com.wade.ops.harmonius;

import com.wade.ops.util.DateUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class OpsHBaseAPI implements Constants {

    private static final Log LOG = LogFactory.getLog(OpsHBaseAPI.class);

    private static final int SIZE_LIMIT = 1000;

    private static Configuration configuration = null;
    private static Connection connection = null;
    private static OpsHBaseAPI instance = null;

    private OpsHBaseAPI() {}

    public Connection getConnection() throws IOException {
        return this.connection;
    }

    /**
     * 获取 OpsHBaseAPI 实例
     *
     * @return
     * @throws IOException
     */
    public static final OpsHBaseAPI getInstance() throws IOException {
        if (null == instance) {
            configuration = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(configuration);
            instance = new OpsHBaseAPI();
        }
        return instance;
    }

    public Relation selectRelatByService(String serviceName) throws IOException {

        Relation relation = new Relation();

        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_SINK_SERVICE_RELAT));
        Get get = new Get(Bytes.toBytes(serviceName));
        Result result = table.get(get);

        result.rawCells();
        for (Cell cell : result.rawCells()) {
            String rowkey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            long count = Bytes.toLong(CellUtil.cloneValue(cell));
            String value = String.valueOf(count);

            if (LOG.isInfoEnabled()) {
                LOG.info("rowkey: " + rowkey);
                LOG.info("family: " + family);
                LOG.info("column: " + column);
                LOG.info(" value: " + value);
            }

            String[] slice = StringUtils.split(column, '^');

            if (family.equals("dependService")) {
                Map<String, String> data = new HashMap<>();
                data.put("dependService", slice[0]);
                data.put("date", slice[1]);
                data.put("count", value);
                relation.getDependService().add(data);
            } else if (family.equals("beDependService")) {
                Map<String, String> data = new HashMap<>();
                data.put("beDependService", slice[0]);
                data.put("date", slice[1]);
                data.put("count", value);

                if ("mainservice".equals(slice[1])) {
                    data.put("mainservice", count > 0 ? "true" : "false");
                } else {
                    data.put("mainservice", "false");
                }

                relation.getBeDependService().add(data);
            } else if (family.equals("beDependMenuId")) {
                Map<String, String> data = new HashMap<>();
                data.put("beDependMenuId", slice[0]);
                data.put("date", slice[1]);
                data.put("count", value);
                relation.getBeDependMenuId().add(data);
            }

        }

        return relation;
    }


    /**
     * 根据 traceid 查询对应的追逐数据集合
     *
     * @param traceid
     * @return
     * @throws IOException
     */
    public List<HashMap<String, Object>> selectByTraceId(String traceid) throws IOException {

        List<HashMap<String, Object>> rtn = new ArrayList<>();

        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE));
        Get get = new Get(Bytes.toBytes(traceid));
        Result result = table.get(get);

        for (Cell cell : result.listCells()) {
            HashMap<String, Object> span = SerializationUtils.deserialize(CellUtil.cloneValue(cell));
            rtn.add(span);
        }

        table.close();

        Collections.sort(rtn, new ProbeComparator()); // 按开始时间进行排序
        return rtn;
    }

    /**
     * 根据菜单ID和时间戳，查询满足条件的 traceid
     *
     * @param menuid
     * @return
     * @throws IOException
     */
    public List<String> selectByMenuId(String menuid, String starttime, String endtime) throws Exception {

        String start = String.valueOf(DateUtil.parse(starttime));
        String end = String.valueOf(DateUtil.parse(endtime));
        String startrow = menuid + "^" + start.substring(0, 9); // 时间戳: 1508729999000 只取前9位
        String stoprow  = menuid + "^" + end.substring(0, 9);

        Set<String> set = new HashSet<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_MENU));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(menuid + "^")));

        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);

            if (set.size() > SIZE_LIMIT) {
                break;
            }
        }

        table.close();

        List<String> rtn = new ArrayList<>();
        rtn.addAll(set);

        return rtn;
    }

    /**
     * 根据 operid 和时间戳，查询满足条件的 traceid
     *
     * @param operid
     * @param starttime
     * @param endtime
     * @return
     * @throws Exception
     */
    public List<String> selectByOperId(String operid, String starttime, String endtime) throws Exception {

        String start = String.valueOf(DateUtil.parse(starttime));
        String end = String.valueOf(DateUtil.parse(endtime));
        String startrow = operid + "^" + start.substring(0, 9); // 时间戳: 1508729999000 只取前9位
        String stoprow  = operid + "^" + end.substring(0, 9);

        Set<String> set = new HashSet<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_OPERID));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(operid + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > SIZE_LIMIT) {
                break;
            }
        }

        table.close();

        List<String> rtn = new ArrayList<>();
        rtn.addAll(set);

        return rtn;
    }

    /**
     * 根据手机号码和时间戳，查询满足条件的 traceid
     *
     * @param sn
     * @param starttime
     * @param endtime
     * @return
     * @throws Exception
     */
    public List<String> selectBySn(String sn, String starttime, String endtime) throws Exception {

        String start = String.valueOf(DateUtil.parse(starttime));
        String end = String.valueOf(DateUtil.parse(endtime));
        String startrow = sn + "^" + start.substring(0, 9); // 时间戳: 1508729999000 只取前9位
        String stoprow  = sn + "^" + end.substring(0, 9);

        Set<String> set = new HashSet<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_SN));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(sn + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > SIZE_LIMIT) {
                break;
            }
        }

        table.close();

        List<String> rtn = new ArrayList<>();
        rtn.addAll(set);

        return rtn;
    }

    /**
     * 根据服务名和时间戳，查询满足条件的 traceid
     *
     * @param servicename
     * @param starttime
     * @param endtime
     * @return
     * @throws Exception
     */
    public List<String> selectByService(String servicename, String starttime, String endtime) throws Exception {

        String start = String.valueOf(DateUtil.parse(starttime));
        String end = String.valueOf(DateUtil.parse(endtime));
        String startrow = servicename + "^" + start.substring(0, 9); // 时间戳: 1508729999000 只取前9位
        String stoprow  = servicename + "^" + end.substring(0, 9);

        Set<String> set = new HashSet<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_SERVICE));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(servicename + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > SIZE_LIMIT) {
                break;
            }
        }

        table.close();

        List<String> rtn = new ArrayList<>();
        rtn.addAll(set);

        return rtn;
    }

    /**
     * 多维度查询
     *
     * @param dimensions
     * @param starttime
     * @param endtime
     * @return
     * @throws Exception
     */
    public List<Map<String, String>> selectDetailsByAll(Map<String, String> dimensions, String starttime, String endtime) throws Exception {

        List<Map<String, String>> rtn = new ArrayList<>();

        List<String> traceids = selectByAll(dimensions, starttime, endtime);
        for (String traceid : traceids) {
            List<HashMap<String, Object>> probes = selectByTraceId(traceid);

            Map<String, String> data = seek(probes);
            data.put("traceid", traceid);
            rtn.add(data);
        }

        return rtn;

    }

    /**
     * 开始时间，完成时间，耗时，工号，菜单，手机号码，源地址。
     *
     * @param probes
     * @return
     */
    private Map<String, String> seek(List<HashMap<String, Object>> probes) {

        String starttime = "";
        String endtime = "";
        long costtime = 0;
        String operid = "";
        String menuid = "";
        String clientip = "";
        String sn = "";
        String bizid = "";

        Map<String, String> rtn = new HashMap<>();
        for (HashMap<String, Object> probe : probes) {

            String sCosttime = (String) probe.get("costtime");
            long costtimeTemp = Long.parseLong(sCosttime);
            if (costtimeTemp > costtime) {
                costtime = costtimeTemp;
                starttime = (String) probe.get("starttime");
                endtime = (String) probe.get("endtime");
            }

            String probetype = (String) probe.get("probetype");

            if ("app".equals(probetype)) {
                if (StringUtils.isBlank(sn)) {
                    Map<String, String> ext = (Map<String, String>) probe.get("ext");
                    if (null != ext) {
                        String snTemp = ext.get("SERIAL_NUMBER");
                        sn = (null != snTemp) ? snTemp : "";
                    }
                }
            }

            if ("web".equals(probetype)) {
                menuid = (String) probe.get("menuid");
                clientip = (String) probe.get("clientip");

                if (StringUtils.isBlank(sn)) {
                    Map<String, String> ext = (Map<String, String>) probe.get("ext");
                    if (null != ext) {
                        String snTemp = ext.get("SERIAL_NUMBER");
                        sn = (null != snTemp) ? snTemp : "";
                    }
                }
            }

            operid = get(probe, "operid", operid);
            bizid = get(probe, "bizid", bizid);

        }

        rtn.put("starttime", starttime);
        rtn.put("endtime", endtime);
        rtn.put("costtime", String.valueOf(costtime));
        rtn.put("operid", operid);
        rtn.put("menuid", menuid);
        rtn.put("clientip", clientip);
        rtn.put("sn", sn);
        rtn.put("bizid", bizid);

        return rtn;
    }

    private static final String get(HashMap<String, Object> probe, String key, String defvalue) {

        String value = (String) probe.get(key);

        if (StringUtils.isNotBlank(value)) {
            return value;
        }

        return defvalue;
    }

    /**
     * 多维度查询
     *
     * @param dimensions
     * @param starttime
     * @param endtime
     * @return
     * @throws Exception
     */
    public List<String> selectByAll(Map<String, String> dimensions, String starttime, String endtime) throws Exception {

        String menuid = dimensions.get("menuid");
        String operid = dimensions.get("operid");
        String sn = dimensions.get("sn");
        String servicename = dimensions.get("servicename");

        LOG.info("OpsHBaseAPI.selectByAll(): param dimensions: " + dimensions);

        Set<String> traceSet = new HashSet<>();
        if (StringUtils.isNotBlank(menuid)) {
            List<String> list = selectByMenuId(menuid, starttime, endtime);
            intersection(traceSet, list);
        }

        if (StringUtils.isNotBlank(operid)) {
            List<String> list = selectByOperId(operid, starttime, endtime);
            intersection(traceSet, list);
        }

        if (StringUtils.isNotBlank(sn)) {
            List<String> list = selectBySn(sn, starttime, endtime);
            intersection(traceSet, list);
        }

        if (StringUtils.isNotBlank(servicename)) {
            List<String> list = selectByService(servicename, starttime, endtime);
            intersection(traceSet, list);
        }

        List<String> rtn = new ArrayList<>();
        rtn.addAll(traceSet);

        if (rtn.size() > 500) {
            rtn = rtn.subList(0, 500);
        }

        LOG.info("OpsHBaseAPI.selectByAll(): return: " + rtn);

        return rtn;
    }

    /**
     * 取交集
     *
     * @param traceSet
     * @param list
     */
    private static final void intersection(Set<String> traceSet, List<String> list) {

        Set<String> set = new HashSet<>();
        set.addAll(list);

        if (0 == traceSet.size()) {
            if (0 != set.size()) {
                traceSet.addAll(set);
            }
        } else {
            traceSet.retainAll(set);
        }

    }

    public static void main(String[] args) throws Exception {

        String action = args[0];
        System.out.println("action: " + action);

        OpsHBaseAPI api = OpsHBaseAPI.getInstance();

        if (action.equals("selectByTraceId")) { // 根据 traceid 查追踪记录
            List<HashMap<String, Object>> spans = api.selectByTraceId(args[1]);
            for (HashMap<String, Object> span : spans) {
                System.out.println(span.get("id") + "|" + span.get("probetype"));
                System.out.println(span);
            }
        } else if (action.equals("selectByMenuId")) { // 根据菜单查追踪ID
            System.out.println("args[1]: " + args[1]);
            System.out.println("args[2]: " + args[2]);
            System.out.println("args[3]: " + args[3]);
            List<String> tids = api.selectByMenuId(args[1], args[2], args[3]);
            for (String tid : tids) {
                System.out.println(tid);
            }
        } else if (action.equals("selectByOperId")) { // 根据工号查看追踪ID
            List<String> tids = api.selectByOperId(args[1], args[2], args[3]);
            for (String tid : tids) {
                System.out.println(tid);
            }
        } else if (action.equals("selectBySn")) { // 根据手机号码查看追踪ID
            List<String> tids = api.selectBySn(args[1], args[2], args[3]);
            for (String tid : tids) {
                System.out.println(tid);
            }
        } else if (action.equals("selectByService")) {
            List<String> tids = api.selectByService(args[1], args[2], args[3]);
            for (String tid : tids) {
                System.out.println(tid);
            }
        } else if (action.equals("selectByAll")) {

            Map<String, String> dimensions = new HashMap<>();
            String queryString = args[1];

            String[] params = StringUtils.split(queryString, '&');
            for (String kv : params) {
                String[] x = StringUtils.split(kv, '=');
                dimensions.put(x[0], x[1]);
            }
            System.out.println("dimensions: " + dimensions);

            List<String>  tids = api.selectByAll(dimensions, args[2], args[3]);
            for (String tid : tids) {
                System.out.println(tid);
            }
        } else if (action.equals("selectDetailsByAll")) {
            Map<String, String> dimensions = new HashMap<>();
            String queryString = args[1];

            String[] params = StringUtils.split(queryString, '&');
            for (String kv : params) {
                String[] x = StringUtils.split(kv, '=');
                dimensions.put(x[0], x[1]);
            }
            System.out.println("dimensions: " + dimensions);
            List<Map<String, String>> details = api.selectDetailsByAll(dimensions, args[2], args[3]);
            for (Map<String, String> detail : details) {
                System.out.println(detail);
            }

        } else if (action.equals("selectRelatByService")) {
            Relation relation = api.selectRelatByService(args[1]);
            System.out.println(relation);
        }

    }
}
