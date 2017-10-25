package com.wade.ops;

import com.wade.ops.util.DateUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    public static final OpsHBaseAPI getIntance() throws IOException {
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

        HTable table = (HTable) connection.getTable(TableName.valueOf("trace"));
        Get get = new Get(Bytes.toBytes(traceid));
        Result result = table.get(get);

        for (Cell cell : result.listCells()) {
            HashMap<String, Object> span = SerializationUtils.deserialize(CellUtil.cloneValue(cell));
            rtn.add(span);
        }

        table.close();
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

        List<String> rtn = new ArrayList<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf("trace_menu"));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(menuid + "^")));

        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            rtn.add(tid);
        }

        table.close();
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

        List<String> rtn = new ArrayList<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf("trace_operid"));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(operid + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            rtn.add(tid);
        }

        table.close();
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

        List<String> rtn = new ArrayList<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf("trace_sn"));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(sn + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            rtn.add(tid);
        }

        table.close();
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

        List<String> rtn = new ArrayList<>();
        HTable table = (HTable) connection.getTable(TableName.valueOf("trace_service"));

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(servicename + "^")));
        scan.setStartRow(Bytes.toBytes(startrow));
        scan.setStopRow(Bytes.toBytes(stoprow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            byte[] byteValue = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid"));
            String tid = Bytes.toString(byteValue);
            rtn.add(tid);
        }

        table.close();
        return rtn;
    }

    public static void main(String[] args) throws Exception {

        String action = args[0];

        OpsHBaseAPI api = OpsHBaseAPI.getIntance();

        if (action.equals("selectByTraceId")) { // 根据 traceid 查追踪记录
            List<HashMap<String, Object>> spans = api.selectByTraceId(args[1]);
            for (HashMap<String, Object> span : spans) {
                System.out.println(span.get("id") + "|" + span.get("probetype"));
            }
            //System.out.println(spans);
        } else if (action.equals("selectByMenuId")) { // 根据菜单查追踪ID
            System.out.println("args[2]: " + args[2]);
            System.out.println("args[3]: " + args[3]);
            List<String> tids = api.selectByMenuId(args[1], args[2], args[3]);
            //for (String tid : tids) {
            //    System.out.println(tid);
            //}
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
        }

    }
}
