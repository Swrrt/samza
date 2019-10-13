package org.apache.samza.controller.streamswitch;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface StreamSwitchMetricsRetriever {
    void init();
    //Metrics should be in Map <Type of metrics, Map <ContainerId, Data> > format
    Map<String, Object> retrieveMetrics();
}
