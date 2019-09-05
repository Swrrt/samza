package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.JobControllerListener;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Under development

public class DelayGuaranteeStreamSwitch extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(DelayGuaranteeStreamSwitch.class);
    class MigrationContext{
        Map<String, String> partitionToMove;
    }
    public DelayGuaranteeStreamSwitch(Config config){
        super(config);
    }
    @Override
    protected boolean updateModel(Map<String, Object> metrics){
        LOG.info("Updating model from metrics");
        Map<String, String> taskArrived = (HashMap<String, String>)(metrics.get("TaskArrived"));
        Map<String, String> taskProcessed = (HashMap<String, String>)(metrics.get(""))
        return false;
    };
    @Override
    public synchronized void onLastChangeImplemented(){
        if(waitForMigrationDeployed){

            waitForMigrationDeployed = false;
        }
    }
}
