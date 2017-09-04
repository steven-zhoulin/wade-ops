package com.wade.ops.harmonius.crawler.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 主配置类对象
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public class Config {

    /**
     * bomc文件基础目录
     */
    private String bomcBaseDirectory;

    /**
     * 爬取文件线程池大小, 默认:5
     */
    private int crawlerPoolsize = 5;

    /**
     * 加载文件线程池大小, 默认:5
     */
    private int loadingPoolsize = 5;

    /**
     * 默认超时时间
     */
    private int defaultTimeout;

    /**
     * 默认端口
     */
    private int defaultPort;

    /**
     * 默认密码
     */
    private String defaultPswd;

    /**
     * 主机列表
     */
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

    public int getCrawlerPoolsize() {
        return crawlerPoolsize;
    }

    public void setCrawlerPoolsize(int crawlerPoolsize) {
        this.crawlerPoolsize = crawlerPoolsize;
    }

    public int getLoadingPoolsize() {
        return loadingPoolsize;
    }

    public void setLoadingPoolsize(int loadingPoolsize) {
        this.loadingPoolsize = loadingPoolsize;
    }

    public String getBomcBaseDirectory() {
        return bomcBaseDirectory;
    }

    public void setBomcBaseDirectory(String bomcBaseDirectory) {
        this.bomcBaseDirectory = bomcBaseDirectory;
    }
}
