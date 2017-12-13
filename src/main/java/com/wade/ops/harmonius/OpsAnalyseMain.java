package com.wade.ops.harmonius;

import com.wade.ops.harmonius.loader.HBaseUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 分析
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class OpsAnalyseMain {

    private static final Log LOG = LogFactory.getLog(OpsAnalyseMain.class);

    private static final byte[] CF_MENUID = Bytes.toBytes("menuid");
    private static final byte[] CF_RELAT = Bytes.toBytes("relat");
    private static final byte[] COL_POSITIVE = Bytes.toBytes("positive");
    private static final byte[] COL_REVERSE = Bytes.toBytes("reverse");

    /**
     * 抽取待分析的菜单集
     *
     * @return
     * @throws IOException
     */
    private Map<String, String> extractAnalyseMenu() throws IOException {

        Map<String, String> map = new HashMap<>();

        Connection connection = OpsHBaseAPI.getInstance().getConnection();
        HTable table = (HTable) connection.getTable(TableName.valueOf(Constants.HT_TRACE_MENU));

        Scan scan = new Scan();
        long timestamp = System.currentTimeMillis() - 1000000; // 分析前1000秒的数据
        timestamp -= (timestamp % 1000000);
        String now = Long.toString(timestamp).substring(0, 7);

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("^" + now));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String rowkey = Bytes.toString(r.getRow());
            // 这里需要通过时间戳过滤一部分数据

            String tid = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid")));
            int i = rowkey.indexOf('^');
            rowkey = rowkey.substring(0, i + 7); // 8: 每一千秒分析一次, 7: 每一万秒分析一次,
            if (map.containsKey(rowkey)) {
                continue;
            }

            map.put(rowkey, tid);

        }

        table.close();
        return map;
    }

    /**
     * 分析服务关系
     *
     * @param menus
     * @throws Exception
     */
    private void analyseServiceRelation(Map<String, String> menus) throws Exception {

        for (String key : menus.keySet()) {

            String menuid = key.substring(0, key.indexOf('^'));
            String traceid = menus.get(key);
            List<HashMap<String, Object>> probes = OpsHBaseAPI.getInstance().selectByTraceId(traceid);
            List<HashMap<String, Object>> serviceProbes = new ArrayList<>();

            // 剔除非service的span
            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("service".equals(probetype)) {
                    serviceProbes.add(probe);
                }
            }

            int i = 0;
            for (HashMap<String, Object> parent : serviceProbes) {

                String id =  (String) parent.get("id");
                String pServiceName = (String) parent.get("servicename");
                for (HashMap<String, Object> child : serviceProbes) {
                    String parentid =  (String) child.get("parentid");
                    String cServicename = (String) child.get("servicename");
                    String starttime = (String) child.get("starttime");
                    if (id.equals(parentid)) {
                        loadServieRelat(starttime, pServiceName, cServicename, menuid);
                        if (i++ % 1000 == 0) {
                            HBaseUtils.serviceMapFlushCommits();
                        }
                    }
                }
            }

            HBaseUtils.serviceMapFlushCommits();
        }
    }

    /**
     * 加载服务依赖
     *
     * @param starttime 开始时间
     * @param pServiceName 上级服务名
     * @param cServiceName 下级服务名
     */
    private void loadServieRelat(String starttime, String pServiceName, String cServiceName, String menuid) throws Exception {

        long st = Long.parseLong(starttime);
        String day = DateFormatUtils.format(st, "yyyyMM");

        byte[] qualifier = Bytes.toBytes(starttime);
        byte[] value = Bytes.toBytes(menuid);

        // 正向服务依赖
        Put positivePut = new Put(Bytes.toBytes(pServiceName + "^" + day));
        positivePut.addColumn(CF_RELAT, COL_POSITIVE, Bytes.toBytes(cServiceName));
        positivePut.addColumn(CF_MENUID, qualifier, value);
        HBaseUtils.serviceMapPut(positivePut);

        // 反向服务依赖
        Put reversePut = new Put(Bytes.toBytes(cServiceName + "^" + day));
        reversePut.addColumn(CF_RELAT, COL_REVERSE, Bytes.toBytes(pServiceName));
        reversePut.addColumn(CF_MENUID, qualifier, value);
        HBaseUtils.serviceMapPut(reversePut);

    }

    public static void main(String[] args) {

        OpsAnalyseMain serviceMapAnalyse = new OpsAnalyseMain();

        while (true) {
            try {
                LOG.info("start service relationship analysing...");
                long start = System.currentTimeMillis();
                Map<String, String> map = serviceMapAnalyse.extractAnalyseMenu();
                serviceMapAnalyse.analyseServiceRelation(map);
                LOG.info("analyse completed, cost: " + (System.currentTimeMillis() - start) + "ms");
                Thread.sleep(1000 * 1000); // 一千秒分析一次
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}