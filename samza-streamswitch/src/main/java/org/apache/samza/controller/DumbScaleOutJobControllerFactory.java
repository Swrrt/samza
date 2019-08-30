package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbScaleOutJobControllerFactory implements JobControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutJobControllerFactory.class);

    @Override
    public JobController getController(Config config) {
        return new DumbScaleOutJobController(config);
    }
}
