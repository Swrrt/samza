package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.JobController;
import org.apache.samza.controller.JobControllerListener;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class StreamSwitch implements JobController {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitch.class);
    Config config;
    JobControllerListener listener;
    StreamSwitchMetricsRetriever retriever;
    Map<String, List<String>> partitionAssignment;
    boolean waitForMigrationDeployed;
    long startTime = 0;
    public StreamSwitch(Config config){
        this.config = config;
    }
    @Override
    public void init(JobControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        this.retriever = createMetricsRetriever();
        this.retriever.init();
    }

    private StreamSwitchMetricsRetriever createMetricsRetriever(){
        String retrieverFactoryClassName = config.getOrDefault("streamswitch.metrics.factory", "org.apache.samza.controller.streamswitch.DefaultRetrieverFactory");
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
                //To prevent migration deployment during update process, use synchronization lock.
                synchronized (this) {
                    if (updateModel(time, metrics)) {
                        if (!waitForMigrationDeployed) waitForMigrationDeployed = true;
                        else {
                            LOG.info("Waring: new migration before last migration is deployed! Ignore old migration");
                            //TODO: throw an error?
                        }
                    }
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
