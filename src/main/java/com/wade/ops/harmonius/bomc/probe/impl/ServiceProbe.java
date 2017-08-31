package com.wade.ops.harmonius.bomc.probe.impl;

import com.wade.ops.harmonius.bomc.probe.AbstractProbe;

public class ServiceProbe extends AbstractProbe {
	
	private String serviceName;
	private boolean mainService = false;

	public boolean isMainService() {
		return this.mainService;
	}

	public void setMainService(boolean mainService) {
		this.mainService = mainService;
	}

	public String getServiceName() {
		return this.serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String toString() {
		return super.toString();
	}
}