package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DumbScaleOutOperatorController implements OperatorController {
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutOperatorController.class);

    OperatorControllerListener listener;
    Config config;
    int startNumber = 1;
    List<String> executors, partitions;
    public DumbScaleOutOperatorController(Config config){
        this.config = config;
        executors = null;
        partitions = null;
    }
    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        this.partitions = new ArrayList(partitions);
        this.executors = new ArrayList<>(executors);
        LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);
        startNumber = executors.size();
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
        LOG.info("Initial executorMapping: " + partitionAssignment);
        listener.remap(partitionAssignment);
    }
    @Override
    public void start(){
        LOG.info("Start Job Controller");
        while(true){
            tryToScale();
        }
    }
    void tryToScale(){
        for(int i=startNumber+1;i<=partitions.size();i++) {
            try{
                Thread.sleep(120000);
                LOG.info("Try to scale out");
                while(executors.size()<i){
                    executors.add(String.format("%06d", executors.size()+2));
                }
                Map<String, List<String>> partitionAssignment  = new HashMap<>();
                for(int j=0;j<executors.size();j++){
                    partitionAssignment.put(executors.get(j), new LinkedList<>());
                }
                int j=0;
                for(String partition: partitions){
                    partitionAssignment.get(executors.get(j)).add(partition);
                    j++;
                    if(j>=executors.size())j=0;
                }
                listener.scale(i, partitionAssignment);
            }catch(Exception e){
            }
        }
    }
    @Override
    public void onMigrationExecutorsStopped(){
    }
    @Override
    public void onMigrationCompleted(){
    }
}
