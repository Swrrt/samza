package org.apache.samza.streamswitch;

import java.util.List;
import java.util.Map;

public interface StreamSwitchListener {
    void changePartitionAssignment(Map<String, List<String>> partitionToExecutor);
    void scaling(int parallelism, Map<String, List<String>> partitionToExecutor);
}
