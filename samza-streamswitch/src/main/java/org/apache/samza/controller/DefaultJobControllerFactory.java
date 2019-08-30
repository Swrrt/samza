package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultJobControllerFactory implements JobControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobControllerFactory.class);

    @Override
    public JobController getController(Config config) {
        return new DefaultJobController(config);
    }
}
