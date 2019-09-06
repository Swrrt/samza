package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultJobController implements JobController {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobController.class);
    JobControllerListener listener;
    Config config;
    public DefaultJobController(Config config){
        this.config = config;
    }
    @Override
    public void init(JobControllerListener listener, List<String> partitions, List<String> executors){
        this.listener = listener;
    }
    @Override
    public void start(){
        LOG.info("Start stream switch");

        LOG.info("DefaultJobController does nothing, quit");
    }

    @Override
    public void onChangeImplemented(){
    }
}
