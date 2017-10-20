package com.wade.ops.harmonius;

import org.apache.log4j.Logger;

import java.util.UUID;

public class FlumeAppenderTest {

    private static final Logger LOG = Logger.getLogger(FlumeAppenderTest.class);

    public static void main(String[] args) throws InterruptedException {

        while (true) {

            Thread.sleep(1000);

            String msg = UUID.randomUUID().toString();
            LOG.info(msg);
            LOG.debug(msg);
            LOG.warn(msg);
            LOG.error(msg);
            System.out.println("running...");

        }
    }

}
