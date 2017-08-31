package com.wade.ops.harmonius.bomc.probe;

public abstract class AbstractProbe implements IProbe {
	
	private String probeType;
	private String id;
	private String parentId;
	private String traceId;
	private String bizId;
	private String operId;
	private String starttime;
	private String endtime;
	private String costtime;

	private boolean success;

	public String getProbeType() {
		return this.probeType;
	}

	public void setProbeType(String probeType) {
		this.probeType = probeType;
	}

	public void setTraceId(String traceId) {
		this.traceId = traceId;
	}

	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParentId() {
		return this.parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public String getTraceId() {
		return this.traceId;
	}

	public String getBizId() {
		return this.bizId;
	}

	public void setBizId(String bizId) {
		this.bizId = bizId;
	}

	public void setOperId(String operId) {
		this.operId = operId;
	}

	public String getOperId() {
		return this.operId;
	}

	public String getStarttime() {
		return this.starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}

	public String getEndtime() {
		return this.endtime;
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
	
	public boolean isSuccess() {
		return this.success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("probeType:" + probeType + ", ");
		sb.append("id:" + id + ", ");
		sb.append("parentId:" + parentId + ", ");
		sb.append("traceId:" + traceId + ", ");
		sb.append("bizId:" + bizId + ", ");
		sb.append("operId:" + operId + ", ");
		sb.append("starttime:" + starttime + ", ");
		sb.append("endtime:" + endtime + ", ");
		sb.append("costtime:" + costtime);
		return sb.toString();
	}
}
