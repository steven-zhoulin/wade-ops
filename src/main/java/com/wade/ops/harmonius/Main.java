package com.wade.ops.harmonius;

import com.alibaba.fastjson.JSON;
import com.wade.ops.harmonius.crawler.FileCrawlerScheduler;
import com.wade.ops.harmonius.crawler.config.Config;
import com.wade.ops.harmonius.loader.FileLoaderScheduler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: create 'bomc', 'info'
 * @auth: steven.zhou
 * @date: 2017/08/31
 */
public class Main {

    public static Config config = null;

    public static final Map<String, CrawlState> STATES = new HashMap<>();

    /**
     * 加载配置文件
     *
     * @throws IOException
     */
    Config loadConfig() throws IOException {

        System.out.println("loading configuration config.json...");
        InputStream in = Main.class.getResourceAsStream("/config.json");
        byte[] data = IOUtils.toByteArray(in);
        String content = new String(data);
        return JSON.parseObject(content.trim(), Config.class);

    }

    /**
     * 加载资源
     *
     * @throws Exception
     */
    private void load() throws Exception {

        ProtectionDomain domain = Main.class.getProtectionDomain();
        CodeSource codeSource = domain.getCodeSource();
        URL loc = codeSource.getLocation();

        if (null == loc) {
            throw new NullPointerException("获取启动位置发生错误!");
        }

        String absolutePath = null;
        File startJarFile = new File(loc.getFile());
        File startJarDirectory = null;
        if (startJarFile.isFile()) {
            absolutePath = startJarFile.getAbsolutePath();

            int idx = absolutePath.lastIndexOf(File.separatorChar);
            if (idx > -1) {
                startJarDirectory = new File(absolutePath.substring(0, idx));

            }
        }

        System.out.println("启动位置为:    " + absolutePath);
        System.out.println("jar包所在目录: " + startJarDirectory.toString());

        if (null == startJarDirectory || !startJarDirectory.isDirectory()) {
            return;
        }

        URLClassLoader loader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
        Class<URLClassLoader> loaderClass = URLClassLoader.class;
        Method method = loaderClass.getDeclaredMethod("addURL", new Class[] { URL.class });
        method.setAccessible(true);

        File[] files = startJarDirectory.listFiles();
        if (null == files || files.length <= 0) {
            return;
        }

        System.out.println("开始加载jar包文件...");

        Arrays.sort(files);
        for (File file : files) {
            String filePath = file.getAbsolutePath();
            if (filePath.endsWith(".jar")) {

                if (filePath.equals(absolutePath)) {
                    continue;
                }

                URL url = file.toURI().toURL();
                method.invoke(loader, url);
                System.out.println("loading " + filePath);
            }
        }

    }

    public static void main(String[] args) throws Exception {

        Main main = new Main();
        main.load();

        config = main.loadConfig();

        System.out.println("crawler pool size: " + config.getCrawlerPoolsize());
        System.out.println("loading pool size: " + config.getLoadingPoolsize());

        FileCrawlerScheduler fileCrawlerScheduler = new FileCrawlerScheduler(config.getCrawlerPoolsize());
        fileCrawlerScheduler.start();

        FileLoaderScheduler fileLoaderScheduler = new FileLoaderScheduler(config.getLoadingPoolsize());
        fileLoaderScheduler.start();

        // [OK] 获取当前时间戳的10分钟归属位置。
        // [OK] 读取web,app主机的地址、账号、密码、目录位置、文件特征标识。
        // 多线程到服务器上通过FTP或SFTP爬bomc.*.dat文件下来。
        // 支持一边爬文件下来，一边录入HBase
        // 以时间戳建目录: .dat.crawling -> .dat -> .dat.loading -> .dat.loaded
        // 按时间戳打包压缩处理，定时删除。

        // 仅用于测试
        //fileCrawlerScheduler.shutdown();
        //fileLoaderScheduler.shutdown();
    }
}
