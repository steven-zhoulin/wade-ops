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
import java.util.*;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 分析
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class OpsAnalyse implements Constants {

    private static final Log LOG = LogFactory.getLog(OpsAnalyse.class);

    /**
     * 抽取待分析的追踪ID集合
     *
     * @return
     * @throws IOException
     */
    private Set<String> extractAnalyseService() throws IOException {

        Set<String> traceIdSet = new HashSet<>();

        Connection connection = OpsHBaseAPI.getInstance().getConnection();
        HTable table = (HTable) connection.getTable(TableName.valueOf(HT_TRACE_SERVICE));

        Scan scan = new Scan();
        long timestamp = System.currentTimeMillis() - 1000000; // 分析前1000秒的数据
        timestamp -= (timestamp % 1000000);
        String now = Long.toString(timestamp).substring(0, 7);

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("^" + now));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            // String rowkey = Bytes.toString(r.getRow());
            String tid = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("tid")));
            traceIdSet.add(tid);
        }

        table.close();
        return traceIdSet;
    }

    /**
     * 带计数器功能的服务依赖分析
     *
     * @param traceIdSet
     * @throws Exception
     */
    private void analyseServiceRelation(Set<String> traceIdSet) throws Exception {

        int i = 0;
        for (String traceid : traceIdSet) {

            LOG.info("待分析traceid: " + traceid);

            List<HashMap<String, Object>> probes = OpsHBaseAPI.getInstance().selectByTraceId(traceid);
            List<HashMap<String, Object>> serviceProbes = new ArrayList<>();

            String menuid = null;

            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("service".equals(probetype)) {
                    serviceProbes.add(probe); // 剔除非service的span
                } else if ("web".equals(probetype)) {
                    menuid = (String) probe.get("menuid"); // 找菜单ID
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
                            HBaseUtils.sinkServiceRelatFlushCommits();
                        }
                    }
                }
            }

            HBaseUtils.sinkServiceRelatFlushCommits();

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
        String yyyyMMdd = DateFormatUtils.format(st, "yyyy-MM-dd");
        String yyyyMM = yyyyMMdd.substring(0, 6);
        String yyyy = yyyyMMdd.substring(0, 4);

        // 依赖的服务计数器
        Increment dependServiceIncrement = new Increment(Bytes.toBytes(pServiceName));
        dependServiceIncrement.addColumn(Bytes.toBytes("dependService"), Bytes.toBytes(cServiceName + "^" + yyyyMMdd), 1);
        dependServiceIncrement.addColumn(Bytes.toBytes("dependService"), Bytes.toBytes(cServiceName + "^" + yyyyMM), 1);
        dependServiceIncrement.addColumn(Bytes.toBytes("dependService"), Bytes.toBytes(cServiceName + "^" + yyyy), 1);
        HBaseUtils.sinkServiceRelatIncrement(dependServiceIncrement);

        // 被依赖的服务计数器
        Increment beDependServiceIncrement = new Increment(Bytes.toBytes(cServiceName));
        beDependServiceIncrement.addColumn(Bytes.toBytes("beDependService"), Bytes.toBytes(pServiceName + "^" + yyyyMMdd), 1);
        beDependServiceIncrement.addColumn(Bytes.toBytes("beDependService"), Bytes.toBytes(pServiceName + "^" + yyyyMM), 1);
        beDependServiceIncrement.addColumn(Bytes.toBytes("beDependService"), Bytes.toBytes(pServiceName + "^" + yyyy), 1);
        if (mainservice) {
            beDependServiceIncrement.addColumn(Bytes.toBytes("beDependService"), Bytes.toBytes(pServiceName + "^mainservice"), 1); // 为主服务计数器
        }
        HBaseUtils.sinkServiceRelatIncrement(beDependServiceIncrement);

        // 被依赖的菜单计数器
        Increment beDependMenuIdIncrement = new Increment(Bytes.toBytes(pServiceName));
        beDependMenuIdIncrement.addColumn(Bytes.toBytes("beDependMenuId"), Bytes.toBytes(menuid + "^" + yyyyMMdd), 1);
        beDependMenuIdIncrement.addColumn(Bytes.toBytes("beDependMenuId"), Bytes.toBytes(menuid + "^" + yyyyMM), 1);
        beDependMenuIdIncrement.addColumn(Bytes.toBytes("beDependMenuId"), Bytes.toBytes(menuid + "^" + yyyy), 1);
        HBaseUtils.sinkServiceRelatIncrement(beDependMenuIdIncrement);

    }


    public void start() throws Exception {

        LOG.info("服务关系分析进程启动完成!");

        int i = 0;
        while (true) {

            try {

                long start = System.currentTimeMillis();

                Set<String> traceIdSet = extractAnalyseService(); // 考虑根据trace_service表来分析。
                LOG.info(String.format("开始第%-3d轮分析, 待分析链路共计: %-5d条。", i++, traceIdSet.size()));

                analyseServiceRelation(traceIdSet);

                long cost = System.currentTimeMillis() - start;
                LOG.info(String.format("第%-3d轮分析完成, 耗时: %d ms", i, cost));

                Thread.sleep(1000 * 1000); // 一千秒分析一次
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
