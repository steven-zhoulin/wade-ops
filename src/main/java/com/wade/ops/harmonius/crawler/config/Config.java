package com.wade.ops.harmonius.crawler.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/09/01
 */
public class Config {

    private int defaultTimeout;
    private int defaultPort;
    private String defaultPswd;
    private List<Host> hosts = new ArrayList<Host>();

    public int getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    public int getDefaultPort() {
        return defaultPort;
    }

    public void setDefaultPort(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    public String getDefaultPswd() {
        return defaultPswd;
    }

    public void setDefaultPswd(String defaultPswd) {
        this.defaultPswd = defaultPswd;
    }

    public List<Host> getHosts() {
        return hosts;
    }

    public void setHosts(List<Host> hosts) {
        this.hosts = hosts;
    }

    public void addHost(Host host) {
        this.hosts.add(host);
    }
}
