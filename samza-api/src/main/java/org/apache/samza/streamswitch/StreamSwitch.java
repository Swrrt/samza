package org.apache.samza.streamswitch;

public interface StreamSwitch {
    void init(StreamSwitchListener listener);
    void start();
}
