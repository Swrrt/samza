package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultOperatorController implements OperatorController {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorController.class);
    OperatorControllerListener listener;
    Config config;
    public DefaultOperatorController(Config config){
        this.config = config;
    }
    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
    }
    @Override
    public void start(){
        LOG.info("Start DefaultJobController");
        LOG.info("DefaultJobController does nothing, quit");
    }

    @Override
    public void onMigrationExecutorsStopped(){
    }
    @Override
    public void onMigrationCompleted(){
    }
}
