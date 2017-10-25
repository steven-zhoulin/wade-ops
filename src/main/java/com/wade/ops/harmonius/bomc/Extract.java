package com.wade.ops.harmonius.bomc;

import com.wade.ops.harmonius.bomc.probe.IProbe;
import com.wade.ops.harmonius.bomc.probe.impl.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 可用的traceid: (必须从Browser开始)
 *
 * web-0faeedc05a184347ba946f6b1d459691
 *
 * 思路:
 * 定义一个基于内存的数据结构，把拎出来的数据集喂给它，它就能自动组织好各元素的上下级关系，并打印出来。
 *
 * @author Steven
 *
 */
public class Extract {

	private static Configuration configuration = HBaseConfiguration.create();;
	private static Connection connection = null;

	public static void main(String[] args) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);

		getTraceDatasByTraceId("web-0faeedc05a184347ba946f6b1d459691|");
	}

	private static final void getTraceDatasByTraceId(String traceid) throws IOException {
		HTable table = (HTable) connection.getTable(TableName.valueOf("bomc"));

		Map<String, IProbe> list = new HashMap<String, IProbe>();

		Scan scan = new Scan();
		scan.setFilter(new PrefixFilter(Bytes.toBytes(traceid)));
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs) {
			System.out.println(r);
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			byte[] family = Bytes.toBytes("info");

			String probetype = new String(r.getValue(family, Bytes.toBytes("probetype")));
			String id = new String(r.getValue(family, Bytes.toBytes("id")));
			String parentid = new String(r.getValue(family, Bytes.toBytes("parentid")));
			// String traceid = new String(r.getValue(family, Bytes.toBytes("traceid")));
			// String bizid = new String(r.getValue(family, Bytes.toBytes("bizid")));
			// String operid = new String(r.getValue(family, Bytes.toBytes("operid")));
			String starttime = new String(r.getValue(family, Bytes.toBytes("starttime")));
			String endtime = new String(r.getValue(family, Bytes.toBytes("endtime")));
			String costtime = new String(r.getValue(family, Bytes.toBytes("costtime")));

			switch (probetype) {
			case "browser":
				BrowserProbe browserProbe = new BrowserProbe();
				browserProbe.setProbeType("browser");
				browserProbe.setId(id);
				browserProbe.setParentId(parentid);
				browserProbe.setTraceId(traceid);
				// browserProbe.setBizId(bizid);
				// browserProbe.setOperId(operid);
				browserProbe.setStarttime(starttime);
				browserProbe.setEndtime(endtime);
				browserProbe.setCosttime(costtime);
				list.put(id, browserProbe);
				break;
			case "web":
				WebProbe webProbe = new WebProbe();
				webProbe.setProbeType("web");
				webProbe.setId(id);
				webProbe.setParentId(parentid);
				webProbe.setTraceId(traceid);
				// webProbe.setBizId(bizid);
				// webProbe.setOperId(operid);
				webProbe.setStarttime(starttime);
				webProbe.setEndtime(endtime);
				webProbe.setCosttime(costtime);
				list.put(id, webProbe);
				break;
			case "app":
				AppProbe appProbe = new AppProbe();
				appProbe.setProbeType("app");
				appProbe.setId(id);
				appProbe.setParentId(parentid);
				appProbe.setTraceId(traceid);
				// appProbe.setBizId(bizid);
				// appProbe.setOperId(operid);
				appProbe.setStarttime(starttime);
				appProbe.setEndtime(endtime);
				appProbe.setCosttime(costtime);
				list.put(id, appProbe);
				break;
			case "service":
				ServiceProbe serviceProbe = new ServiceProbe();
				serviceProbe.setProbeType("service");
				serviceProbe.setId(id);
				serviceProbe.setParentId(parentid);
				serviceProbe.setTraceId(traceid);
				// serviceProbe.setBizId(bizid);
				// serviceProbe.setOperId(operid);
				serviceProbe.setStarttime(starttime);
				serviceProbe.setEndtime(endtime);
				serviceProbe.setCosttime(costtime);
				list.put(id, serviceProbe);
				break;
			case "dao":
				DaoProbe daoProbe = new DaoProbe();
				daoProbe.setProbeType("dao");
				daoProbe.setId(id);
				daoProbe.setParentId(parentid);
				daoProbe.setTraceId(traceid);
				// daoProbe.setBizId(bizid);
				// daoProbe.setOperId(operid);
				daoProbe.setStarttime(starttime);
				daoProbe.setEndtime(endtime);
				daoProbe.setCosttime(costtime);
				list.put(id, daoProbe);
				break;
			}

		}
		System.out.println("####################################################");
		display(list);
		System.out.println("####################################################");
	}
	
	/**
	 * Key: parentid, Value: [ʵ��1��ʵ��2��ʵ��3...]
	 * Key: parentid, Value: [ʵ��1��ʵ��2��ʵ��3...]
	 * 
	 * @param map
	 */
	private static void display(Map<String, IProbe> map) {
		
		Map<String, List<IProbe>> tree = new HashMap<String, List<IProbe>>();

		IProbe root = null;
		for (String id : map.keySet()) {
			IProbe probe = map.get(id);
			String parentid = probe.getParentId();
			List<IProbe> list = tree.get(parentid);
			if (null == list) {
				list = new ArrayList<IProbe>();
				tree.put(parentid, list);
			}
			list.add(probe);
			
			if ("root".equals(parentid)) {
				root = probe;
			}
		}
		
		
		xx(root, tree);
	}
	
	private static void xx(IProbe probe, Map<String, List<IProbe>> tree) {
		System.out.println(probe);
		List<IProbe> list = tree.get(probe.getId());
		if (null == list || list.size() == 0) {
			return;
		}
		
		for (IProbe ipe : list) {
			xx(ipe, tree);
		}
	}
}
