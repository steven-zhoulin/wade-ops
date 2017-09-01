package com.wade.ops.harmonius.crawler;

import com.jcraft.jsch.*;

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

    public static void crawlBomcFiles(String host, String user, String pswd, String path, String sPort) throws JSchException, SftpException, FileNotFoundException {

        int port = Integer.parseInt(sPort);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        JSch jsch = new JSch();
        Session session = jsch.getSession(host, user, port);
        session.setPassword(pswd);
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

}
