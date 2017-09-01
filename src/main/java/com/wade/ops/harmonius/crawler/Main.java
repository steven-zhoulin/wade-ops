package com.wade.ops.harmonius.crawler;

import com.alibaba.fastjson.JSON;
import com.jcraft.jsch.*;
import com.wade.ops.harmonius.crawler.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Properties;
import java.util.Vector;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/08/31
 */
public class Main {

    private static Log LOG = LogFactory.getLog(Main.class);

    public static void main(String[] args) throws IOException, SftpException, JSchException {

        System.out.println("Main.main");
        LOG.info("-- INFO --" + timestamp());
        LOG.debug("-- DEBUG --" + timestamp());

        InputStream in = Main.class.getResourceAsStream("/config.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);

        Config config = JSON.parseObject(content.trim(), Config.class);
        System.out.println("timeout: " + config.getDefaultTimeout());
        System.out.println(config.getHosts());

        /*
        List<Host> hosts = JSON.parseArray(content.trim(), Host.class);
        for (Host host : hosts) {
            System.out.println(host);
            //downloadBomcFiles(host.getHost(), host.getUser(), host.getPswd(), host.getPath());
        }
        */

        // [OK] 获取当前时间戳的10分钟归属位置。
        // [OK] 读取web,app主机的地址、账号、密码、目录位置、文件特征标识。
        // 多线程到服务器上通过FTP或SFTP爬bomc.*.dat文件下来。
        // 爬下来的文件改文件后缀名为 .dat.fin 表示已经下载完毕。
        // ...
        // 往HBase灌入数据。
        //
    }

    private static void downloadBomcFiles(String host, String user, String pswd, String path) throws JSchException, SftpException, FileNotFoundException {

        JSch jsch = new JSch();
        Session session = jsch.getSession(user, host, 22);
        session.setPassword(pswd);

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        session.setConfig(config);
        session.setTimeout(5000);
        session.connect();

        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
        channel.cd(path);
        Vector bomcs = channel.ls("bomc*.dat");

        for (Object o : bomcs) {
            if (o instanceof ChannelSftp.LsEntry) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;
                String fileName = entry.getFilename();
                File file = new File("D:\\bomc\\" + fileName);
                channel.get(fileName, new FileOutputStream(file));
                System.out.println("downloading " + fileName + " success!");
            }
        }

        channel.quit();
        channel.disconnect();
        channel.getSession().disconnect();
        session.disconnect();
    }

    /**
     * 获取当前时间戳，格式: MMddHHm0
     *
     * @return
     */
    private static final String timestamp() {

        String timestamp = DateFormatUtils.format(System.currentTimeMillis(), "MMddHHmm");
        return timestamp.substring(0, 7) + "0";

    }

}
