package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// Under development
public class TestJobModelChangeOperatorController implements OperatorController {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobModelChangeOperatorController.class);

    OperatorControllerListener listener;
    Config config;
    Map<String, List<String>> partitionAssignment;
    public TestJobModelChangeOperatorController(Config config){
        this.config = config;
    }
    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions){
        LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);
        this.listener = listener;
        partitionAssignment = new HashMap();
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
        LOG.info("Start stream switch");
        while(true){
            tryToMove();
        }
    }

    void tryToMove(){
        int moveTimes = 10;
        long warmUp = 30000;
        try{
            LOG.info("Waiting " + warmUp + "ms for initilization...");
            Thread.sleep(warmUp);
        }catch (Exception e){
            LOG.info("Exception when warmUp : " + e);
        }
        for(int i=0;i<moveTimes;i++){
            try{
                Thread.sleep(75000);
                Random rand = new Random();
                int x = rand.nextInt(partitionAssignment.size());
                LOG.info("Try to migrate one partition");
                String tgtContainer = null;
                String srcContainer = null;
                for(String containerId: partitionAssignment.keySet()){
                    if(x == 0){
                        tgtContainer = containerId;
                        x = -1;
                    }else{
                        x--;
                        if(srcContainer == null && partitionAssignment.get(containerId).size() > 1){
                            srcContainer = containerId;
                        }
                    }
                }
                if(srcContainer != null && tgtContainer != null && !srcContainer.equals(tgtContainer)){
                    LOG.info("Migrate partition " + partitionAssignment.get(srcContainer).get(0) +  " from " + srcContainer + " to " + tgtContainer);
                    partitionAssignment.get(tgtContainer).add(partitionAssignment.get(srcContainer).get(0));
                    partitionAssignment.get(srcContainer).remove(0);
                }
                listener.remap(partitionAssignment);
            }catch (Exception e){
                LOG.info(e.getMessage());
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
