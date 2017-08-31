package com.wade.ops.harmonius.bomc;

import org.apache.commons.io.DirectoryWalker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * create 'bomc', 'info'
 * 
 * @author Steven
 *
 */
public class DataExtraction {

	private static final byte[] FAMILY_INFO = Bytes.toBytes("info");

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
	
	
	private static final Configuration configuration = HBaseConfiguration.create();;
	private static Connection connection = null;
	private static HTable table = null;
	
	public static void main(String[] args) throws IOException {
		
		connection = ConnectionFactory.createConnection(configuration);
		table = (HTable) connection.getTable(TableName.valueOf("bomc"));
		table.setAutoFlushTo(false);

		ListFileWorker worker = new ListFileWorker();
		
		//List<File> files = worker.list(new File("D:\\eclipse-workspace\\hello\\bomc"));
		List<File> files = worker.list(new File("/home/hbase/bomc-load/bomc-files"));
		
		for (File file : files) {
			process(file);
		}

		table.flushCommits();
		table.close();
		
	}
	
	@SuppressWarnings("unchecked")
	private static void process(File file) throws IOException {

		System.out.println(file);

		FileInputStream fis = new FileInputStream(file);
		ObjectInputStream ois = new ObjectInputStream(fis);
		
		try {
			
			while (true) {
				
				Map<String, Object> map = (Map<String, Object>) ois.readObject();
				
				BaseData baseData = loadBaseTraceData(map);
				String probetype = baseData.getProbetype();
				
				switch (probetype) {
				case "browser":
					loadBrowserTraceData(map, baseData);
					break;
				case "web":
					loadWebTraceData(map, baseData);
					break;
				case "app":
					loadAppTraceData(map, baseData);
					break;
				case "service":
					loadServiceTraceData(map, baseData);
					break;
				case "dao":
					loadDaoTraceData(map, baseData);
					break;
				}
			}
						
		} catch (EOFException ee) {
			System.out.println("�ļ������������!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ois.close();
		}
		

	}

	private static BaseData loadBaseTraceData(Map<String, Object> map) throws IOException {

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

	@SuppressWarnings("unused")
	private static void loadBrowserTraceData(Map<String, Object> map, BaseData baseData) throws IOException {

		// �������
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
		table.put(put);
		table.flushCommits();
	}

	@SuppressWarnings("unused")
	private static void loadWebTraceData(Map<String, Object> map, BaseData baseData) throws IOException {
		
		// �������
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
		
		table.put(put);
		table.flushCommits();
	}

	private static void loadAppTraceData(Map<String, Object> map, BaseData baseData) throws IOException {
		
		// �������
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
		table.put(put);
		table.flushCommits();
	}
	
	private static void loadServiceTraceData(Map<String, Object> map, BaseData baseData) throws IOException {
		// �������
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
		
		table.put(put);
		table.flushCommits();
	}
	
	private static void loadDaoTraceData(Map<String, Object> map, BaseData baseData) throws IOException {
		// �������
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
		
		table.put(put);
		table.flushCommits();
	}
	
	private static class ListFileWorker extends DirectoryWalker<File> {
		private List<File> list(File directory) throws IOException {
			List<File> files = new ArrayList<File>();
			walk(directory, files);
			return files;
		}

		@Override
		protected void handleFile(File file, int depth, Collection<File> results) throws IOException {
			String filename = file.getName();
			if (!filename.startsWith("bomc.")) {
				return;
			}
			
			if (filename.endsWith("11111240.dat") || filename.endsWith("11111230.dat") || filename.endsWith("11111250.dat")) {
				results.add(file);
			}
		}
	}

	private static class BaseData {
		private String traceid;
		private String id;
		private String parentid;
		private String starttime;
		private String endtime;
		private String costtime;
		private String probetype;
		private String bizid;
		private String operid;
		private String success;
		
		public String getTraceid() {
			return traceid;
		}

		public void setTraceid(String traceid) {
			this.traceid = traceid;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getParentid() {
			return parentid;
		}

		public void setParentid(String parentid) {
			this.parentid = parentid;
		}

		public String getStarttime() {
			return starttime;
		}

		public void setStarttime(String starttime) {
			this.starttime = starttime;
		}

		public String getEndtime() {
			return endtime;
		}

		public void setEndtime(String endtime) {
			this.endtime = endtime;
		}

		public String getCosttime() {
			return costtime;
		}

		public void setCosttime(String costtime) {
			this.costtime = costtime;
		}

		public String getProbetype() {
			return probetype;
		}

		public void setProbetype(String probetype) {
			this.probetype = probetype;
		}
		
		public String getBizid() {
			return bizid;
		}

		public void setBizid(String bizid) {
			this.bizid = bizid;
		}

		public String getOperid() {
			return operid;
		}

		public void setOperid(String operid) {
			this.operid = operid;
		}
		
		public String getSuccess() {
			return success;
		}

		public void setSuccess(String success) {
			this.success = success;
		}
	}
}
