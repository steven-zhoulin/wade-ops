package com.wade.ops.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/15
 */
@SuppressWarnings("unused")
public class BomcFileSource extends AbstractSource implements Configurable, PollableSource {

    private String fileName;

    @Override
    public Status process() throws EventDeliveryException {

        File file = new File(this.fileName);

        FileInputStream fis = null;
        ObjectInputStream ois = null;

        try {

            fis = new FileInputStream(file);
            ois = new ObjectInputStream(fis);

            while (true) {

                Map<String, Object> info = (Map<String, Object>) ois.readObject();
                String text = info.toString();

                Map<String, String> header = new HashMap<>();
                header.put("wade.server.name", "app-node01-srv01");
                this.getChannelProcessor().processEvent(EventBuilder.withBody(text, Charset.forName("UTF-8"), header));
                //return Status.READY;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != ois) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return Status.READY;

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        this.fileName = context.getString("fileName");
        System.out.println("#############################################################################");
        System.out.println("fileName: " + this.fileName);
        System.out.println("#############################################################################");
    }

}
