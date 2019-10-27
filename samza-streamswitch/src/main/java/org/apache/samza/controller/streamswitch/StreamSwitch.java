package org.apache.samza.controller.streamswitch;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.controller.JobController;
import org.apache.samza.controller.JobControllerListener;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class StreamSwitch implements JobController {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitch.class);
    Config config;
    JobControllerListener listener;
    StreamSwitchMetricsRetriever retriever;
    Map<String, List<String>> partitionAssignment;
    boolean waitForMigrationDeployed;
    long startTime = 0;
    ReentrantLock updateLock; //Lock is used to avoid concurrent modify between updateModel() and changeImplemented()
    AtomicLong nextExecutorID;

    public StreamSwitch(Config config){
        this.config = config;
    }
    @Override
    public void init(JobControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        this.retriever = createMetricsRetriever();
        this.retriever.init();

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
        listener.changePartitionAssignment(partitionAssignment);
    }

    private StreamSwitchMetricsRetriever createMetricsRetriever(){
        String retrieverFactoryClassName = config.getOrDefault("streamswitch.metrics.factory", "org.apache.samza.controller.streamswitch.JMXMetricsRetrieverFactory");
        return Util.getObj(retrieverFactoryClassName, StreamSwitchMetricsRetrieverFactory.class).getRetriever(config);

    }

    @Override
    public void start(){
        int metricsRetreiveInterval = config.getInt("streamswitch.metrics.interval", 200);
        int triggerInterval = config.getInt("streamswitch.trigger.interval", 200);
        int metricsWarmupTime = config.getInt("streamswitch.metrics.warmup.time", 60000);
        boolean isWarmup = true;
        waitForMigrationDeployed = false;
        startTime = System.currentTimeMillis();
        while(true) {
            long time = System.currentTimeMillis();
            if (time - startTime > metricsWarmupTime) isWarmup = false;
            if (!isWarmup) {
                Map<String, Object> metrics = retriever.retrieveMetrics();
                //To prevent migration deployment during update process, use synchronization updateLock.
                LOG.info("Try to acquire lock to update model...");
                updateLock.lock();
                try {
                    LOG.info("Lock acquired, update model");
                    if (updateModel(time, metrics)) {
                        if (!waitForMigrationDeployed) waitForMigrationDeployed = true;
                        else {
                            LOG.info("Waring: new migration before last migration is deployed! Ignore old migration");
                            //TODO: throw an error?
                        }
                    }
                }finally {
                    updateLock.unlock();
                    LOG.info("Model update completed, unlock");
                }
            }
            try {
                Thread.sleep(metricsRetreiveInterval);
            }catch (Exception e) { }

        }
    }

    @Override
    public synchronized void onChangeImplemented(){
        if(waitForMigrationDeployed){

            waitForMigrationDeployed = false;
        }
    }
    //Need extend class to implement
    //Return true if migration is triggered
    protected boolean updateModel(long time, Map<String, Object> metrics){
        return false;
    };
}
