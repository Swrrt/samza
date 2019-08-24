package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultController implements AbstractController {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultController.class);
    ControllerListener listener;
    Config config;
    public DefaultController(Config config){
        this.config = config;
    }
    @Override
    public void init(ControllerListener listener, List<String> partitions, List<String> executors){
        this.listener = listener;
    }
    @Override
    public void start(){
        LOG.info("Start stream switch");

        LOG.info("DefaultController does nothing, quit");
    }

    @Override
    public void lastChangeImplemented(){
    }
}
