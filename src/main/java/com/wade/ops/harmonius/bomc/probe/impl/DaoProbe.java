package com.wade.ops.harmonius.bomc.probe.impl;

import com.wade.ops.harmonius.bomc.probe.AbstractProbe;

import java.util.Map;

public class DaoProbe extends AbstractProbe {

	private String dataSource;
	private long dccost;
	private String sqlName;
	private String sql;
	private Map<String, Object> sqlParams;

	public String getDataSource() {
		return this.dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public long getDccost() {
		return this.dccost;
	}

	public void setDccost(long dccost) {
		this.dccost = dccost;
	}

	public String getSqlName() {
		return this.sqlName;
	}

	public void setSqlName(String sqlName) {
		this.sqlName = sqlName;
	}

	public String getSql() {
		return this.sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Map<String, Object> getSqlParams() {
		return this.sqlParams;
	}

	public void setParams(Map<String, Object> sqlParams) {
		this.sqlParams = sqlParams;
	}
	
	public String toString() {
		return super.toString();
	}
}