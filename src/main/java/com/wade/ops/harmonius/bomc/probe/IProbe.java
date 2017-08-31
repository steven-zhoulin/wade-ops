package com.wade.ops.harmonius.bomc.probe;

public interface IProbe {
	
	String BROWSER = "browser";
	String WEB = "web";
	String ESB = "esb";
	String APP = "app";
	String SERVICE = "service";
	String DAO = "dao";
	String DATASOURCE = "datasource";
	String ECS = "ecs";
	String IBS = "ibs";
	String UIP = "uip";
	String PF = "pf";

	String getProbeType();

	void setProbeType(String paramString);

	String getId();

	void setId(String paramString);

	String getParentId();

	void setParentId(String paramString);

	String getTraceId();

	void setTraceId(String paramString);

	String getBizId();

	void setBizId(String paramString);

	String getOperId();

	void setOperId(String paramString);

	boolean isSuccess();

	void setSuccess(boolean paramBoolean);

}
