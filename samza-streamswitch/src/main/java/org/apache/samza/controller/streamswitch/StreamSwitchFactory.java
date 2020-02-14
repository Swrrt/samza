package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.OperatorController;
import org.apache.samza.controller.OperatorControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamSwitchFactory implements OperatorControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitchFactory.class);
    public abstract StreamSwitch getController(Config config);
}

