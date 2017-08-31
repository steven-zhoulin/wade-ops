package com.wade.ops.harmonius.bomc.probe.impl;

import com.wade.ops.harmonius.bomc.probe.AbstractProbe;

import java.util.Map;

public class WebProbe extends AbstractProbe {

	private String sessionId;
	private String clientIp;
	private String ip;
	private String serverName;
	private String url;
	private String menuid;
	private Map<String, String> ext;
	
	public String getSessionId() {
		return this.sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getClientIp() {
		return this.clientIp;
	}

	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}

	public String getIp() {
		return this.ip;
	}

	public String getServerName() {
		return this.serverName;
	}

	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getMenuid() {
		return this.menuid;
	}

	public void setMenuid(String menuid) {
		this.menuid = menuid;
	}

	public Map<String, String> getExt() {
		return this.ext;
	}

	public void setExt(Map<String, String> ext) {
		this.ext = ext;
	}
	
	public String toString() {
		return super.toString();
	}
}
