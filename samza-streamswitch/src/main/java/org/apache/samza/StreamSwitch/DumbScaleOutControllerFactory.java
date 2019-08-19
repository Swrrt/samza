package org.apache.samza.StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.AbstractController;
import org.apache.samza.controller.ControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbScaleOutControllerFactory implements ControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutControllerFactory.class);

    @Override
    public AbstractController getController(Config config) {
        return new DumbScaleOutController(config);
    }
}
