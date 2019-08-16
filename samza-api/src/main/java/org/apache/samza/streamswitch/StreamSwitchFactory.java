package org.apache.samza.streamswitch;

import org.apache.samza.config.Config;

public interface StreamSwitchFactory {
    StreamSwitch getStreamSwitch(Config config);
}
