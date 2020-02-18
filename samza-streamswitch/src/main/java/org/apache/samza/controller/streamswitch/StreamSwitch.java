package org.apache.samza.controller.streamswitch;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.controller.OperatorController;
import org.apache.samza.controller.OperatorControllerListener;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class StreamSwitch implements OperatorController{
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitch.class);
    protected OperatorControllerListener listener;
    protected StreamSwitchMetricsRetriever metricsRetriever;
    protected Map<String, List<String>> partitionAssignment;
    protected long migrationInterval, metricsRetreiveInterval;
    protected boolean isMigrating;
    ReentrantLock updateLock; //Lock is used to avoid concurrent modify between calculateModel() and changeImplemented()
    AtomicLong nextExecutorID;
    Config config;

    class Prescription {
        String source, target;
        List<String> migratingPartitions;
        Prescription(){
            migratingPartitions = null;
        }
        Prescription(String source, String target, List<String> migratingPartitions){
            this.source = source;
            this.target = target;
            this.migratingPartitions = migratingPartitions;
        }
        Map<String, List<String>> generateNewPartitionAssignment(Map<String, List<String>> oldAssignment){
            Map<String, List<String>> newAssignment = new HashMap<>();
            for(String executor: oldAssignment.keySet()){
                List<String> partitions = new ArrayList<>(oldAssignment.get(executor));
                newAssignment.put(executor, partitions);
            }
            if (!newAssignment.containsKey(target)) newAssignment.put(target, new LinkedList<>());
            for (String partition : migratingPartitions) {
                newAssignment.get(source).remove(partition);
                newAssignment.get(target).add(partition);
            }
            //For scale in
            if (newAssignment.get(source).size() == 0) newAssignment.remove(source);
            return newAssignment;
        }
    }

    public StreamSwitch(Config config){
        this.config = config;
        migrationInterval = config.getLong("streamswitch.migration.interval.time", 5000l);
        metricsRetreiveInterval = config.getInt("streamswitch.metrics.interval", 100);
        metricsRetriever = createMetricsRetriever();
        isMigrating = false;
        updateLock = new ReentrantLock();
    }
    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        metricsRetriever.init();
        //Default partitionAssignment
        LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);
        partitionAssignment = new HashedMap();
        nextExecutorID = new AtomicLong();
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
        nextExecutorID.set(executors.size() + 2);
        String executor = executors.get(0);
        while(iterator.hasNext()){
            partitionAssignment.get(executor).add(iterator.next());
        }
        LOG.info("Initial partitionAssignment: " + partitionAssignment);
        listener.remap(partitionAssignment);
    }

    @Override
    public void start(){
        int metricsWarmupTime = config.getInt("streamswitch.metrics.warmup.time", 60000);
        long startTime = System.currentTimeMillis();
        //Warm up phase
        LOG.info("Warm up for " + metricsWarmupTime + " milliseconds...");
        do{
            long time = System.currentTimeMillis();
            if(time - startTime > metricsWarmupTime){
                break;
            }
            else {
                try{
                    Thread.sleep(500l);
                }catch (Exception e){
                    LOG.error("Exception happens during warming up, ", e);
                }
            }
        }while(true);
        LOG.info("Warm up completed.");
        long timeIndex = 1;
        boolean stopped = false;
        while(!stopped) {
            long time = System.currentTimeMillis();
            //Actual calculation, decision here
            work(time, timeIndex);

            //Calculate # of time slots we have to skip due to longer calculation
            long deltaT = System.currentTimeMillis() - time;
            long skippedSlots = (deltaT - 1)/ metricsRetreiveInterval;
            timeIndex += skippedSlots + 1;
            if(skippedSlots > 0){
                LOG.warn("Run loop time (" + deltaT + ") is longer than interval(" + metricsRetreiveInterval + "), please consider to set larger interval");
            }
            LOG.info("Sleep for " + ((skippedSlots + 1) * metricsRetreiveInterval - deltaT) + "milliseconds");
            try{
                Thread.sleep((skippedSlots + 1) * metricsRetreiveInterval - deltaT);
            }catch (Exception e) {
                LOG.warn("Exception happens between run loop interval, ", e);
                stopped = true;
            }
        }
        LOG.info("Stream switch stopped");
    }
    abstract void work(long time, long timeIndex);
    protected StreamSwitchMetricsRetriever createMetricsRetriever(){
        String retrieverFactoryClassName = config.getOrDefault("streamswitch.metrics.factory", "org.apache.samza.controller.streamswitch.JMXMetricsRetrieverFactory");
        return Util.getObj(retrieverFactoryClassName, StreamSwitchMetricsRetrieverFactory.class).getRetriever(config);
    }
}
