package com.wade.ops.harmonius.bomc.probe.impl;

import com.wade.ops.harmonius.bomc.probe.AbstractProbe;

import java.util.Map;

public class AppProbe extends AbstractProbe {

    private String ip;
    private String serverName;
    private Map<String, String> ext;

    public String getIp() {
        return this.ip;
    }

    public String getServerName() {
        return this.serverName;
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