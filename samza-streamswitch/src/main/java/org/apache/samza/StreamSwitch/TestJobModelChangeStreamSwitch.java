package org.apache.samza.StreamSwitch;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// Under development
public class TestJobModelChangeStreamSwitch implements StreamSwitch{
    private static final Logger LOG = LoggerFactory.getLogger(TestJobModelChangeStreamSwitch.class);

    StreamSwitchListener listener;
    Config config;
    Map<String, List<String>> partitionAssignment;
    public TestJobModelChangeStreamSwitch(Config config){
        this.config = config;
    }
    @Override
    public void init(StreamSwitchListener listener){
        this.listener = listener;
    }
    @Override
    public void setPartitionsAndExecutors(List<String> partitions, List<String> executors){
        partitionAssignment = new HashedMap();
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
        for(int i=0;i<moveTimes;i++){
            try{
                Thread.sleep(30000);
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
                listener.changePartitionAssignment(partitionAssignment);
            }catch (Exception e){
                LOG.info("Exception: " + e.toString());
            }

        }
    }
}