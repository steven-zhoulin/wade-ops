package com.wade.ops.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AbstractSource;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/15
 */
public class BomcFileSource2 implements Configurable, EventDrivenSource {


    @Override
    public void setChannelProcessor(ChannelProcessor channelProcessor) {

    }

    @Override
    public ChannelProcessor getChannelProcessor() {
        return null;
    }

    @Override
    public void setName(String s) {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public LifecycleState getLifecycleState() {
        return null;
    }

}
