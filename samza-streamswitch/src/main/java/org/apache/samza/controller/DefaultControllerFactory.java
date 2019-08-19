package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultControllerFactory implements ControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultControllerFactory.class);

    @Override
    public AbstractController getController(Config config) {
        return new DefaultController(config);
    }
}
