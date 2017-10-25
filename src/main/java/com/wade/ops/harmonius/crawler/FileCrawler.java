package com.wade.ops.harmonius.crawler;

import com.jcraft.jsch.*;
import com.wade.ops.harmonius.OpsLoadMain;
import com.wade.ops.config.Host;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.Vector;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.zhou
 * @date: 2017/09/01
 */
class FileCrawler extends Thread {

    private static final Log LOG = LogFactory.getLog(FileCrawler.class);
    private static final String CRLF = System.getProperty("line.separator");

    private Host host;
    private String timestamp;
    private String dstDir;

    FileCrawler(Host host, String timestamp, String dstDir) {
        this.host = host;
        this.timestamp = timestamp;
        this.dstDir = dstDir;
    }

    @Override
    public void run() {
        try {
            crawlFilesBySftp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过SFTP协议从远程主机爬bomc文件
     *
     * @throws Exception
     */
    private void crawlFilesBySftp() {

        int count = 0;

        String ip = host.getHost();
        String user = host.getUser();
        int port = host.getPort();
        String path = host.getPath();
        int timeout = OpsLoadMain.config.getDefaultTimeout();

        Properties prop = new Properties();
        prop.put("StrictHostKeyChecking", "no");

        Session session = null;
        ChannelSftp channel = null;

        try {

            JSch jsch = new JSch();
            session = jsch.getSession(user, ip, port);
            session.setPassword(host.getPswd());
            session.setTimeout(timeout);
            session.setConfig(prop);
            session.connect();

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd(path);

            LOG.info("Resource from " + host.getUser() + "@" + host.getHost() + ":" + host.getPath());

            Vector bomcs = channel.ls("bomc.*" + timestamp + ".dat");

            for (Object o : bomcs) {
                if (o instanceof ChannelSftp.LsEntry) {
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;
                    String fileName = entry.getFilename();
                    File file = new File(dstDir + "/" + fileName + ".crawling");
                    FileOutputStream fos = new FileOutputStream(file);

                    long begin = System.currentTimeMillis();
                    channel.get(fileName, fos);
                    long cost = System.currentTimeMillis() - begin;

                    IOUtils.closeQuietly(fos);
                    long size = FileUtils.sizeOf(file);
                    //LOG.info(String.format("crawl %s@%s:%s/%-45s %6d bytes cost %3s ms", host.getUser(), host.getHost(), host.getPath(), fileName, size, cost));
                    file.renameTo(new File(dstDir + "/" + fileName));
                    count++;
                }
            }

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        } finally {

            if (null != channel) {
                channel.quit();
                channel.disconnect();
                try {
                    channel.getSession().disconnect();
                } catch (JSchException e) {
                    e.printStackTrace();
                }
            }

            if (null != session) {
                session.disconnect();
            }
        }

    }

}
