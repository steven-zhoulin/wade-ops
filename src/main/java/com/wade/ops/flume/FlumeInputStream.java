package com.wade.ops.flume;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.serialization.ResettableInputStream;

import java.io.IOException;
import java.io.InputStream;

public class FlumeInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(FlumeInputStream.class);

    private final ResettableInputStream in;

    public FlumeInputStream(ResettableInputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        try {
            return this.in.read();
        } catch (Exception e) {
            LOG.error("input stream read failed:" + e.getMessage());
            return 0;
        }
    }

}
