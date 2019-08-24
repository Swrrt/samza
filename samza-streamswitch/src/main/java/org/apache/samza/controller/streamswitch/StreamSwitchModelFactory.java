package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;

public interface StreamSwitchModelFactory {

    StreamSwitchModel getModel(Config config);
}
