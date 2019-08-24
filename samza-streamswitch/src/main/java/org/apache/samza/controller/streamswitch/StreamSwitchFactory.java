package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.AbstractController;
import org.apache.samza.controller.ControllerFactory;
import org.apache.samza.controller.DefaultController;
import org.apache.samza.controller.DefaultControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSwitchFactory implements ControllerFactory{
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitchFactory.class);

    @Override
    public AbstractController getController(Config config) {
        return new StreamSwitch(config);
    }
}
