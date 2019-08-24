package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JMXMetricsRetriever implements StreamSwitchMetricsRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.samza.controller.streamswitch.JMXMetricsRetriever.class);

    Config config;
    public JMXMetricsRetriever(Config config){
        this.config = config;
    }
    @Override
    public void init(){

    }
    @Override
    public Map<String, Object> retrieveMetrics(){
        return null;
    }
}
