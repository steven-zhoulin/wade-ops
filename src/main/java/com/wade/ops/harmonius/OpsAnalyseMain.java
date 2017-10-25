package com.wade.ops.harmonius;

import com.wade.ops.harmonius.loader.HBaseUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
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

public class OpsAnalyseMain {

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

        Connection connection = OpsHBaseAPI.getIntance().getConnection();
        HTable table = (HTable) connection.getTable(TableName.valueOf("trace_menu"));

        Scan scan = new Scan();
        long timestamp = System.currentTimeMillis();
        timestamp -= (timestamp % 1000000);
        String now = Long.toString(timestamp).substring(0, 7);

        now = "150";
        //     1508844648216
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("^" + now));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String rowkey = Bytes.toString(r.getRow());
            // 这里需要通过时间戳过滤调一部分数据

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

            String traceid = menus.get(key);
            List<HashMap<String, Object>> probes = OpsHBaseAPI.getIntance().selectByTraceId(traceid);
            List<HashMap<String, Object>> serviceProbes = new ArrayList<>();

            // 剔除非service的span
            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("service".equals(probetype)) {
                    serviceProbes.add(probe);
                }
            }

            int i = 0;
            for (HashMap<String, Object> serviceProbe : serviceProbes) {

                String id =  (String) serviceProbe.get("id");
                String parentServiceName = (String) serviceProbe.get("servicename");
                for (HashMap<String, Object> sub : serviceProbes) {
                    String parentid =  (String) sub.get("parentid");
                    String childServicename = (String) sub.get("servicename");
                    String starttime = (String) sub.get("starttime");
                    if (id.equals(parentid)) {
                        loadServieRelat(starttime, parentServiceName, childServicename);
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
     * @param starttime
     * @param parentServiceName
     * @param childServiceName
     */
    private void loadServieRelat(String starttime, String parentServiceName, String childServiceName) throws Exception {

        long st = Long.parseLong(starttime);
        String day = DateFormatUtils.format(st, "yyyyMMdd");

        // 正向服务依赖
        Put positivePut = new Put(Bytes.toBytes(parentServiceName + "^" + day));
        positivePut.addColumn(CF_RELAT, COL_POSITIVE, Bytes.toBytes(childServiceName));
        HBaseUtils.serviceMapPut(positivePut);

        // 反向服务依赖
        Put reversePut = new Put(Bytes.toBytes(childServiceName + "^" + day));
        reversePut.addColumn(CF_RELAT, COL_REVERSE, Bytes.toBytes(parentServiceName));
        HBaseUtils.serviceMapPut(reversePut);

    }

    public static void main(String[] args) throws Exception {

        OpsAnalyseMain serviceMapAnalyse = new OpsAnalyseMain();
        Map<String, String> map = serviceMapAnalyse.extractAnalyseMenu();
        serviceMapAnalyse.analyseServiceRelation(map);

    }

}