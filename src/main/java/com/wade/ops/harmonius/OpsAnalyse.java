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
public class OpsAnalyse implements Constants {

    private static final Log LOG = LogFactory.getLog(OpsAnalyse.class);

    private static final byte[] CF_RELAT = Bytes.toBytes("relat");

    /**
     * 抽取待分析的菜单集
     *
     * @return
     * @throws IOException
     */
    private Map<String, String> extractAnalyseMenu() throws IOException {

        Map<String, String> map = new HashMap<>();

        Connection connection = OpsHBaseAPI.getInstance().getConnection();
        HTable table = (HTable) connection.getTable(TableName.valueOf(HT_TRACE_MENU));

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

        int i = 0;
        for (String key : menus.keySet()) {

            String menuid = key.substring(0, key.indexOf('^'));
            String traceid = menus.get(key);
            LOG.info("待分析traceid: " + traceid);

            List<HashMap<String, Object>> probes = OpsHBaseAPI.getInstance().selectByTraceId(traceid);
            List<HashMap<String, Object>> serviceProbes = new ArrayList<>();

            // 剔除非service的span
            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("service".equals(probetype)) {
                    serviceProbes.add(probe);
                }
            }
            LOG.info("  剔除非service span后，待分析的服务集合: " + serviceProbes.size() + "个");

            List<HashMap<String, Object>> tempProbes = new ArrayList<>();
            tempProbes.addAll(serviceProbes);

            for (HashMap<String, Object> parent : serviceProbes) {

                String pServicename = (String) parent.get("servicename");
                String pStarttime = (String) parent.get("starttime");
                Boolean mainservice = (Boolean) parent.get("mainservice");

                for (HashMap<String, Object> child : tempProbes) {
                    String cServicename = (String) child.get("servicename");
                    String cStarttime = (String) child.get("starttime");
                    if (pStarttime.compareTo(cStarttime) < 0) {
                        loadServieRelat(pStarttime, pServicename, cServicename, menuid, mainservice);
                        LOG.info(String.format("  记录服务关系数据: %s, 主服务: %s, 子服务: %s, 菜单: %s", pStarttime, pServicename, cServicename, menuid));
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
     * @param mainservice 是否为主服务
     */
    private void loadServieRelat(String starttime, String pServiceName, String cServiceName, String menuid, Boolean mainservice) throws Exception {

        long st = Long.parseLong(starttime);
        String yyyyMM = DateFormatUtils.format(st, "yyyy-MM");
        String yyyyMMdd = DateFormatUtils.format(st, "yyyy-MM-dd");
        
        byte[] dependServiceBytes = Bytes.toBytes(dependService + cServiceName + "|"+ yyyyMMdd);
        byte[] beDependServiceBytes = Bytes.toBytes(beDependService + pServiceName + "|" + yyyyMMdd);
        byte[] beDependMenuIdBytes = Bytes.toBytes(beDependMenuId + menuid + "|" + yyyyMMdd);

        // 正向服务依赖
        Put positivePut = new Put(Bytes.toBytes(pServiceName + "^" + yyyyMM));
        positivePut.addColumn(CF_RELAT, dependServiceBytes, NULL_BYTES);
        positivePut.addColumn(CF_RELAT, beDependMenuIdBytes, NULL_BYTES);
        HBaseUtils.serviceMapPut(positivePut);

        // 反向服务依赖
        Put reversePut = new Put(Bytes.toBytes(cServiceName + "^" + yyyyMM));
        reversePut.addColumn(CF_RELAT, beDependServiceBytes, Bytes.toBytes("mainservice=" + mainservice));
        reversePut.addColumn(CF_RELAT, beDependMenuIdBytes, NULL_BYTES);
        HBaseUtils.serviceMapPut(reversePut);

    }

    public void start() throws Exception {

        LOG.info("服务地图关系分析进程启动成功!");

        while (true) {
            try {

                long start = System.currentTimeMillis();

                Map<String, String> map = extractAnalyseMenu();
                analyseServiceRelation(map);

                long cost = System.currentTimeMillis() - start;
                LOG.info(String.format("分析完成, 共分析菜单: %d项, 耗时: %d ms", map.size(), cost));

                Thread.sleep(1000 * 1000); // 一千秒分析一次
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
