package org.apache.samza.controller;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class DumbScaleOutJobController implements JobController {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutJobController.class);

    JobControllerListener listener;
    Config config;
    public DumbScaleOutJobController(Config config){
        this.config = config;
    }
    @Override
    public void init(JobControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);
        this.listener = listener;
        HashMap<String, List<String>> partitionAssignment = new HashMap();
        Iterator<String> iterator = partitions.iterator();
        int times = partitions.size() / executors.size();
        for(String executor: executors){
            partitionAssignment.put(executor, new LinkedList<>());
            for(int i=0;i<times;i++){
                if(iterator.hasNext()){
                    partitionAssignment.get(executor).add(iterator.next());
                }
            }
        }
        String executor = executors.get(0);
        while(iterator.hasNext()){
            partitionAssignment.get(executor).add(iterator.next());
        }
        LOG.info("Initial partitionAssignment: " + partitionAssignment);
        listener.changePartitionAssignment(partitionAssignment);
    }
    @Override
    public void start(){
        LOG.info("Start Job Controller");
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
    public void onChangeImplemented(){
    }
}
