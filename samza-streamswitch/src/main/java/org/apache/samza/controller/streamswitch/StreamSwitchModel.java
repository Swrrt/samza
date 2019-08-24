package org.apache.samza.controller.streamswitch;

import org.apache.samza.controller.ControllerListener;

import java.util.List;
import java.util.Map;

public interface StreamSwitchModel {
    void init(StreamSwitch streamSwitch, ControllerListener listener, List<String> executors, List<String> partitions);
    boolean updateMetrics(Map<String, Object> metrics); // Return true for migration or scaling triggered.
    void migrationDeployed();
}
