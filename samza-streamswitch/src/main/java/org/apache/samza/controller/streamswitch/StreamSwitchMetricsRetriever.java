package org.apache.samza.controller.streamswitch;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface StreamSwitchMetricsRetriever {
    void init();
    Map<String, Object> retrieveMetrics();
}
