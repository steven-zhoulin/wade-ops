package com.wade.ops.harmonius.crawler.config;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description: 主机对象
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/08/31
 */
public class Host {

    /**
     * 主机地址
     */
    private String host;

    /**
     * 用户名
     */
    private String user;

    /**
     * 登录密码
     */
    private String pswd;

    /**
     * bomc目录路径
     */
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPswd() {
        return pswd;
    }

    public void setPswd(String pswd) {
        this.pswd = pswd;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "{host:" + host + ", user:" + user + ", pswd:" + pswd + ", path:" + path + "}";
    }
}
