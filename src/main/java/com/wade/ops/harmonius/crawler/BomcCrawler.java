package com.wade.ops.harmonius.crawler;

import com.jcraft.jsch.*;
import com.wade.ops.harmonius.crawler.config.Config;
import com.wade.ops.harmonius.crawler.config.Host;
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
 * @author: steven.chow
 * @date: 2017/09/01
 */
public class BomcCrawler {

    private static final Log LOG = LogFactory.getLog(BomcCrawler.class);

    /**
     * 通过SFTP协议从远程主机爬bomc文件
     *
     * @param config
     * @param host
     * @param timestemp
     * @param dstDir
     * @return
     * @throws JSchException
     * @throws SftpException
     * @throws FileNotFoundException
     */
    public int crawlFilesBySftp(Config config, Host host, String timestemp, String dstDir) throws JSchException, SftpException, FileNotFoundException {

        int count = 0;

        String ip = host.getHost();
        String user = host.getUser();
        int port = host.getPort();
        String path = host.getPath();
        int timeout = config.getDefaultTimeout();

        Properties prop = new Properties();
        prop.put("StrictHostKeyChecking", "no");

        Session session = null;
        ChannelSftp channel = null;

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(ip, user, port);
            session.setPassword(host.getPswd());
            session.setTimeout(timeout);
            session.setConfig(prop);
            session.connect();

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd(path);
            Vector bomcs = channel.ls("bomc.*" + timestemp + ".dat");

            for (Object o : bomcs) {
                if (o instanceof ChannelSftp.LsEntry) {
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) o;
                    String fileName = entry.getFilename();
                    File file = new File(dstDir + "/" + fileName);
                    FileOutputStream fos = new FileOutputStream(file);
                    long begin = System.currentTimeMillis();
                    channel.get(fileName, fos);
                    IOUtils.closeQuietly(fos);
                    long size = FileUtils.sizeOf(file);
                    LOG.info("download " + fileName + ", size: " + size + "bytes, costtime: " + (System.currentTimeMillis() - begin) + "ms");
                    count++;
                }
            }
        } catch (Exception e) {
            LOG.error(e);
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

        return count;
    }

}
