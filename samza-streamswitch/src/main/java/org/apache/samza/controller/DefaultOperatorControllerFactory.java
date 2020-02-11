package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultOperatorControllerFactory implements OperatorControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorControllerFactory.class);

    @Override
    public OperatorController getController(Config config) {
        return new DefaultOperatorController(config);
    }
}
