package com.wade.ops.harmonius.bomc.probe.impl;

import com.wade.ops.harmonius.bomc.probe.AbstractProbe;

public class BrowserProbe extends AbstractProbe {

	private String statuscode;
	private String ieVer;

	public BrowserProbe() {

	}

	public String getStatuscode() {
		return this.statuscode;
	}

	public void setStatuscode(String statuscode) {
		this.statuscode = statuscode;
	}

	public String getIeVer() {
		return this.ieVer;
	}

	public void setIeVer(String ieVer) {
		this.ieVer = ieVer;
	}

	public String toString() {
		return super.toString();
	}
}