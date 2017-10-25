package com.wade.ops.config;

import com.wade.ops.util.TripleDES;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 主机对象
 * @auth: steven.zhou
 * @date: 2017/08/31
 */
public class Host {

    /**
     * 主机地址
     */
    private String host;

    /**
     * 主机端口
     */
    private int port;

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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPswd() {
        return pswd;
    }

    public void setPswd(String pswd) {

        /**
         * 如果为{3DES}密文则进行解密
         */
        if (pswd.startsWith("{3DES}")) {
            this.pswd = TripleDES.decrypt(pswd.substring(6));
        } else {
            this.pswd = pswd;
        }

    }

    @Override
    public String toString() {
        return "{host:" + host + ", port:" + port + ", user:" + user + ", pswd:" + pswd + ", path:" + path + "}";
    }

}
