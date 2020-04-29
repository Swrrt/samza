package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockMetricsRetrieverFactory implements StreamSwitchMetricsRetrieverFactory{
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.samza.controller.streamswitch.StockMetricsRetrieverFactory.class);
    @Override
    public StreamSwitchMetricsRetriever getRetriever(Config config) {
        return new StockMetricsRetriever(config);
    }
}