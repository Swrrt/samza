package org.apache.samza.controller;

import org.apache.samza.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJobModelChangeJobControllerFactory implements JobControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobModelChangeJobControllerFactory.class);

    @Override
    public JobController getController(Config config){
        return new TestJobModelChangeJobController(config);
    }

}
