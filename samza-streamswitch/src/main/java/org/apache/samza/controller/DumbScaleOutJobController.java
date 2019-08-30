package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DumbScaleOutJobController implements JobController {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutJobController.class);

    JobControllerListener listener;
    Config config;
    public DumbScaleOutJobController(Config config){
        this.config = config;
    }
    @Override
    public void init(JobControllerListener listener, List<String> partitions, List<String> executors){
        this.listener = listener;
    }
    @Override
    public void start(){
        LOG.info("Start stream switch");
        while(true){
            tryToScale();
        }
    }
    void tryToScale(){
        int startNumber = 1;
        for(int i=startNumber+1;i<=10;i++) {
            try{
                Thread.sleep(30000);
                LOG.info("Try to scale out");
                listener.scaling(i, null);
            }catch(Exception e){
            }
        }
    }
    @Override
    public void onLastChangeImplemented(){
    }
}
