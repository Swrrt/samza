package org.apache.samza.StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultStreamSwitch implements StreamSwitch{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamSwitch.class);
    StreamSwitchListener listener;
    Config config;
    public DefaultStreamSwitch(Config config){
        this.config = config;
    }
    @Override
    public void init(StreamSwitchListener listener){
        this.listener = listener;
    }
    @Override
    public void setPartitionsAndExecutors(List<String> partitions, List<String> executors){
    }
    @Override
    public void start(){
        LOG.info("Start stream switch");

        LOG.info("DefaultStreamSwitch does nothing, quit");
    }
}
