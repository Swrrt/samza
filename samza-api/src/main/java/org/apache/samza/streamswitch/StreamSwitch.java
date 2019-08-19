package org.apache.samza.streamswitch;

import java.util.List;

public interface StreamSwitch {
    void init(StreamSwitchListener listener);
    void setPartitionsAndExecutors(List<String> partitions, List<String> executors);
    void start();
}
