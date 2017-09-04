package com.wade.ops.harmonius.crawler;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.wade.ops.harmonius.Main;
import com.wade.ops.harmonius.crawler.config.Host;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.Vector;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @description:
 * @version: v1.0
 * @author: steven.chow
 * @date: 2017/09/01
 */
class FileCrawler extends Thread {

    private static final Log LOG = LogFactory.getLog(FileCrawler.class);
    private static final String CRLF = System.getProperty("line.separator");

    private StringBuilder sb = new StringBuilder(1000);
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
    private void crawlFilesBySftp() throws Exception {

        int count = 0;

        String ip = host.getHost();
        String user = host.getUser();
        int port = host.getPort();
        String path = host.getPath();
        int timeout = Main.config.getDefaultTimeout();

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

            sb.append(CRLF).append(CRLF);
            sb.append("Resource from " + host.getUser() + "@" + host.getHost() + ":" + host.getPath()).append(CRLF);

            long begin = System.currentTimeMillis();

            Vector bomcs = channel.ls("bomc.*" + timestamp + ".dat");

            for (Object o : bomcs) {
                if (o instanceof ChannelSftp.LsEntry) {
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;
                    String fileName = entry.getFilename();
                    File file = new File(dstDir + "/" + fileName + ".crawling");
                    FileOutputStream fos = new FileOutputStream(file);

                    long beg = System.currentTimeMillis();
                    channel.get(fileName, fos);
                    long cost = System.currentTimeMillis() - beg;

                    IOUtils.closeQuietly(fos);
                    long size = FileUtils.sizeOf(file);
                    sb.append(String.format("download %-45s %6d bytes cost %3s ms", fileName, size, cost)).append(CRLF);
                    file.renameTo(new File(dstDir + "/" + fileName));
                    count++;
                }
            }

            long cost = System.currentTimeMillis() - begin;
            sb.append("---------------------------------------------------------------").append(CRLF);
            sb.append("crawled file count: " + count + ", cost: " + cost + " ms").append(CRLF);
            LOG.info(sb.toString());
        } finally {

            if (null != channel) {
                channel.quit();
                channel.disconnect();
                channel.getSession().disconnect();
            }

            if (null != session) {
                session.disconnect();
            }
        }

    }

}