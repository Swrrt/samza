package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbScaleOutOperatorControllerFactory implements OperatorControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutOperatorControllerFactory.class);

    @Override
    public OperatorController getController(Config config) {
        return new DumbScaleOutOperatorController(config);
    }
}
