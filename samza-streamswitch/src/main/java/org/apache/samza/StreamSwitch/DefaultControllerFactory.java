package org.apache.samza.StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.AbstractController;
import org.apache.samza.controller.ControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultControllerFactory implements ControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultControllerFactory.class);

    @Override
    public AbstractController getController(Config config) {
        return new DefaultController(config);
    }
}
