package com.wade.ops.harmonius;

import com.wade.ops.harmonius.loader.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 分析
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class OpsAnalyse implements Constants {

    private static final Log LOG = LogFactory.getLog(OpsAnalyse.class);

    private static final Map<String, RelationBuf> RELAT_BUFF = new HashMap<>();

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
     * 分析服务依赖关系
     *
     * @param traceIdSet
     * @throws Exception
     */
    private void analyseServiceRelation(Set<String> traceIdSet) throws Exception {

        for (String traceid : traceIdSet) {

            List<HashMap<String, Object>> probes = OpsHBaseAPI.getInstance().selectByTraceId(traceid);

            String menuid = "";
            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("web".equals(probetype)) {
                    menuid = (String) probe.get("menuid"); // 找菜单ID
                }
            }

            Map<String, HashMap<String, Object>> idMap = buildIdProbe(probes);

            for (HashMap<String, Object> probe : probes) {
                String probetype =  (String) probe.get("probetype");
                if ("service".equals(probetype)) {
                    String servicename = (String) probe.get("servicename");
                    String parentid = (String) probe.get("parentid");
                    traceUpRelationship(servicename, menuid, parentid, idMap); // 根据parentid往上追溯
                }
            }

        }

        loadServieRelat();

    }

    /**
     * 网上溯源依赖关系
     *
     * @param cServicename
     * @param menuid
     * @param parentid
     * @param idMap
     * @throws Exception
     */
    private void traceUpRelationship(String cServicename, String menuid, String parentid, Map<String, HashMap<String, Object>> idMap) throws Exception {

        HashMap<String, Object> probe = idMap.get(parentid);
        if (null == probe) {
            return;
        }

        String probetype =  (String) probe.get("probetype");

        if ("service".equals(probetype)) {

            String pServicename = (String) probe.get("servicename");
            String pStarttime = (String) probe.get("starttime");
            Boolean mainservice = (Boolean) probe.get("mainservice");
            sinkServiceRelat(pStarttime, pServicename, cServicename, menuid, mainservice);

            String pParentid = (String) probe.get("parentid");
            traceUpRelationship(pServicename, menuid, pParentid, idMap); // 递归

        }

    }

    /**
     * 构造 id -> probe 的映射关系
     *
     * @param probes
     * @return
     */
    private Map<String, HashMap<String, Object>> buildIdProbe(List<HashMap<String, Object>> probes) {

        Map<String, HashMap<String, Object>> rtn = new HashMap<>();

        for (HashMap<String, Object> probe : probes) {
            String id = (String) probe.get("id");
            rtn.put(id, probe);
        }

        return rtn;
    }

    private void sinkServiceRelat(String starttime, String pServiceName, String cServiceName, String menuid, Boolean mainservice) {

        long st = Long.parseLong(starttime);
        String yyyyMMdd = DateFormatUtils.format(st, "yyyy-MM-dd");
        String yyyyMM = yyyyMMdd.substring(0, 7);
        String yyyy = yyyyMMdd.substring(0, 4);

        RelationBuf relationBuf = RELAT_BUFF.get(pServiceName);

        Map<String, AtomicLong> dependService = relationBuf.getDependService();
        increment(dependService, cServiceName + "^" + yyyyMMdd);
        increment(dependService, cServiceName + "^" + yyyyMM);
        increment(dependService, cServiceName + "^" + yyyy);

        Map<String, AtomicLong> beDependService =relationBuf.getBeDependService();
        increment(beDependService, pServiceName + "^" + yyyyMMdd);
        increment(beDependService, pServiceName + "^" + yyyyMM);
        increment(beDependService, pServiceName + "^" + yyyy);
        if (mainservice) {
            increment(beDependService, pServiceName + "^" + mainservice);
        }

        if (StringUtils.isNotBlank(menuid)) {
            Map<String, AtomicLong> beDependMenuId = relationBuf.getBeDependMenuId();
            increment(beDependMenuId, menuid + "^" + yyyyMMdd);
            increment(beDependMenuId, menuid + "^" + yyyyMM);
            increment(beDependMenuId, menuid + "^" + yyyy);
        }

    }

    private static final void increment(Map<String, AtomicLong> map, String key) {

        AtomicLong count = map.get(key);

        if (null == count) {
            count = new AtomicLong(0);
            map.put(key, count);
        }

        count.incrementAndGet();
    }

    /**
     * 入HBase表
     *
     * @throws Exception
     */
    private void loadServieRelat() throws Exception {

        for (String servicename : RELAT_BUFF.keySet()) {
            RelationBuf relationBuf = RELAT_BUFF.get(servicename);
            Map<String, AtomicLong> dependService = relationBuf.getDependService();
            Map<String, AtomicLong> beDependService = relationBuf.getBeDependService();
            Map<String, AtomicLong> beDependMenuId = relationBuf.getBeDependMenuId();

            // 依赖的服务计数器
            Increment dependServiceIncrement = new Increment(Bytes.toBytes(servicename));
            for (String qualifier : dependService.keySet()) {
                long count = dependService.get(qualifier).get();
                dependServiceIncrement.addColumn(Bytes.toBytes("dependService"), Bytes.toBytes(qualifier), count);
            }
            HBaseUtils.sinkServiceRelatIncrement(dependServiceIncrement);

            // 被依赖的服务计数器
            Increment beDependServiceIncrement = new Increment(Bytes.toBytes(servicename));
            for (String qualifier : beDependService.keySet()) {
                long count = beDependService.get(qualifier).get();
                beDependServiceIncrement.addColumn(Bytes.toBytes("beDependService"), Bytes.toBytes(qualifier), count);
            }
            HBaseUtils.sinkServiceRelatIncrement(beDependServiceIncrement);

            // 被依赖的菜单计数器
            Increment beDependMenuIdIncrement = new Increment(Bytes.toBytes(servicename));
            for (String qualifier : beDependMenuId.keySet()) {
                long count = beDependMenuId.get(qualifier).get();
                beDependMenuIdIncrement.addColumn(Bytes.toBytes("beDependMenuId"), Bytes.toBytes(qualifier), count);
            }
            HBaseUtils.sinkServiceRelatIncrement(beDependMenuIdIncrement);

        }

        // 显式提交
        HBaseUtils.sinkServiceRelatFlushCommits();

    }

    public void start() throws Exception {

        LOG.info("服务关系分析进程启动完成!");

        int i = 0;
        while (true) {
            i++;
            try {

                long start = System.currentTimeMillis();

                Set<String> traceIdSet = extractAnalyseService(); // 考虑根据trace_service表来分析。
                LOG.info(String.format("开始第%d轮分析, 待分析链路共计: %-5d条。", i, traceIdSet.size()));

                analyseServiceRelation(traceIdSet);

                long cost = System.currentTimeMillis() - start;
                LOG.info(String.format("第%d轮分析完成, 耗时: %d ms", i, cost));

                Thread.sleep(1000 * 1000); // 一千秒分析一次
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
