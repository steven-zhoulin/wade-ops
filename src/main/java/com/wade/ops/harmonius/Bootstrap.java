package com.wade.ops.harmonius;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Arrays;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/14
 */
public class Bootstrap {

    public File getStartJarDirectory() {
        return startJarDirectory;
    }

    private File startJarDirectory;

    /**
     * 加载资源
     *
     * @throws Exception
     */
    public void boot() throws Exception {

        ProtectionDomain domain = OpsLoadMain.class.getProtectionDomain();
        CodeSource codeSource = domain.getCodeSource();
        URL loc = codeSource.getLocation();

        if (null == loc) {
            throw new NullPointerException("获取启动位置发生错误!");
        }

        String absolutePath = null;
        File startJarFile = new File(loc.getFile());

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
}
