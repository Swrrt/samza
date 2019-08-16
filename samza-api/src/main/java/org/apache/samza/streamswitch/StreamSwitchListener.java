package org.apache.samza.streamswitch;

import java.util.Map;

public interface StreamSwitchListener {
    void changeJobModel(Object jobModel);
    void scaling(int parallelism, Object jobModel);
}
