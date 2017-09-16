package com.wade.ops.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/15
 */
public class SpoolDirectorySource extends AbstractSource implements Configurable, EventDrivenSource {

    @Override
    public void configure(Context context) {

    }

    
}
