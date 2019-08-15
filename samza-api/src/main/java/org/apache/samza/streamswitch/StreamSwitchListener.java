package org.apache.samza.streamswitch;

import java.util.Map;

public interface StreamSwitchListener {
    void changeJobMode(Map<String, String> taskToContainer);
    void scaling(int parallelism, Map<String, String> taskToContainer);
}
