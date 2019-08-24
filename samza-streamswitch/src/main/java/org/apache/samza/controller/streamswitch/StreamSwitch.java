package org.apache.samza.controller.streamswitch;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.samza.config.Config;
import org.apache.samza.controller.AbstractController;
import org.apache.samza.controller.ControllerFactory;
import org.apache.samza.controller.ControllerListener;
import org.apache.samza.controller.DefaultController;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

//Under development

public class StreamSwitch implements AbstractController {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitch.class);
    Config config;
    ControllerListener listener;
    StreamSwitchModel model;
    StreamSwitchMetricsRetriever retriever;
    boolean waitForMigrationDeployed;

    public StreamSwitch(Config config){
        this.config = config;
    }
    @Override
    public void init(ControllerListener listener, List<String> executors, List<String> partitions){
        this.listener = listener;
        this.retriever = createRetriever();
        this.retriever.init();
        this.model = createModel();
        this.model.init(this, listener, executors, partitions);

    }



    private StreamSwitchModel createModel(){
        String modelFactoryClassName = config.getOrDefault("streamswitch.model.factory", "org.apache.samza.controller.streamswitch.DefaultModelFactory");
        return Util.getObj(modelFactoryClassName, StreamSwitchModelFactory.class).getModel(config);
    }
    private StreamSwitchMetricsRetriever createRetriever(){
        String retrieverFactoryClassName = config.getOrDefault("streamswitch.metrics.factory", "org.apache.samza.controller.streamswitch.DefaultRetrieverFactory");
        return Util.getObj(retrieverFactoryClassName, StreamSwitchMetricsRetrieverFactory.class).getRetriever(config);

    }

    @Override
    public void start(){
        int metricsRetreiveInterval = config.getInt("streamswitch.metrics.interval", 200);
        int triggerInterval = config.getInt("streamswitch.trigger.interval", 200);
        int warmupTime = config.getInt("streamswitch.warmup.time", 60000);
        boolean isWarmup = true;
        waitForMigrationDeployed = false;
        long startTime = System.currentTimeMillis();
        while(true) {

            long time = System.currentTimeMillis();
            if (time - startTime > warmupTime) isWarmup = false;
            if (!isWarmup) {
                Map<String, Object> metrics = retriever.retrieveMetrics();

                //To prevent migration deployment during update process, use synchronization lock.
                synchronized (this) {
                    if (model.updateMetrics(metrics)) {
                        if (!waitForMigrationDeployed) waitForMigrationDeployed = true;
                        else {
                            LOG.info("Waring: new migration before last migration is deployed! Ignore new migration");
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
    public synchronized void lastChangeImplemented(){
        if(waitForMigrationDeployed){
            model.migrationDeployed();
            waitForMigrationDeployed = false;
        }
    }
}
