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
    protected Map<String, List<String>> executorMapping;
    protected Map<String, Long> oeUnlockTime;
    protected long metricsRetreiveInterval;
    protected int maxNumberOfExecutors;
    protected boolean isMigrating;
    protected long startTime;
    ReentrantLock updateLock; //Lock is used to avoid concurrent modify between update() and changeImplemented()
    AtomicLong nextExecutorID;
    Config config;

    public StreamSwitch(Config config){
        this.config = config;
        metricsRetreiveInterval = config.getInt("streamswitch.system.metrics_interval", 100);
        maxNumberOfExecutors = config.getInt("streamswitch.system.max_executors", 64);
        metricsRetriever = createMetricsRetriever();
        isMigrating = false;
        updateLock = new ReentrantLock();
    }
    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        metricsRetriever.init();
        //Default executorMapping
        LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);
        executorMapping = new HashedMap();
        oeUnlockTime = new HashMap<>();
        nextExecutorID = new AtomicLong();
        Iterator<String> iterator = partitions.iterator();
        for(String executor: executors){
            executorMapping.put(executor, new LinkedList<>());
            for(int i = 0; i < partitions.size() / executors.size(); i++){
                if(iterator.hasNext()){
                    executorMapping.get(executor).add(iterator.next());
                }
            }
        }
        nextExecutorID.set(executors.size() + 2); //Samza's container ID start from 000002
        String executor = executors.get(0);
        while(iterator.hasNext()){
            executorMapping.get(executor).add(iterator.next());
        }
        LOG.info("Initial executorMapping: " + executorMapping);
        listener.remap(executorMapping);
    }

    @Override
    public void start(){
        int metricsWarmupTime = config.getInt("streamswitch.system.warmup_time", 60000);
        long warmUpStartTime = System.currentTimeMillis();
        //Warm up phase
        LOG.info("Warm up for " + metricsWarmupTime + " milliseconds...");
        System.out.println("Start time=" + System.currentTimeMillis());
        do{
            long time = System.currentTimeMillis();
            if(time - warmUpStartTime > metricsWarmupTime){
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

        //Start running
        startTime = System.currentTimeMillis() - metricsRetreiveInterval; //current time is time index 1, need minus a interval to index 0
        long timeIndex = 1;
        boolean stopped = false;
        while(!stopped) {
            //Actual calculation, decision here
            //Should not invoke any thing when updating!
            updateLock.lock();
            try{
                work(timeIndex);
            }finally {
                updateLock.unlock();
            }
            //Calculate # of time slots we have to skip due to longer calculation
            long deltaT = System.currentTimeMillis() - startTime;
            long nextIndex = deltaT / metricsRetreiveInterval + 1;
            long sleepTime = nextIndex * metricsRetreiveInterval - deltaT;
            if(nextIndex > timeIndex + 1){
                LOG.info("Skipped to index:" + nextIndex + " from index:" + timeIndex + " deltaT=" + deltaT);
            }
            timeIndex = nextIndex;
            LOG.info("Sleep for " + sleepTime + "milliseconds");
            try{
                Thread.sleep(sleepTime);
            }catch (Exception e) {
                LOG.warn("Exception happens between run loop interval, ", e);
                stopped = true;
            }
        }
        LOG.info("Stream switch stopped");
    }
    abstract void work(long timeIndex);
    protected StreamSwitchMetricsRetriever createMetricsRetriever(){
        String retrieverFactoryClassName = config.getOrDefault("streamswitch.system.metrics_retriever.factory", "org.apache.samza.controller.streamswitch.JMXMetricsRetrieverFactory");
        return Util.getObj(retrieverFactoryClassName, StreamSwitchMetricsRetrieverFactory.class).getRetriever(config);
    }
}
