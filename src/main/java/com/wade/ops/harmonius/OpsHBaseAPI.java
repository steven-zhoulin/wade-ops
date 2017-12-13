package com.wade.ops.harmonius;

import com.wade.ops.util.DateUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
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
public class OpsHBaseAPI {

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

            if (set.size() > 5000) {
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

        int i = 0;
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > 5000) {
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

        int i = 0;
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > 5000) {
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

        int i = 0;
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            set.add(tid);
            if (set.size() > 5000) {
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
    public List<String> selectByAll(Map<String, String> dimensions, String starttime, String endtime) throws Exception {

        String menuid = dimensions.get("menuid");
        String operid = dimensions.get("operid");
        String sn = dimensions.get("sn");
        String servicename = dimensions.get("servicename");

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
            traceSet.addAll(set);
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
        }

    }
}
