package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.ControllerListener;
import org.apache.samza.controller.DefaultController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


//Under development

public class DelayGuaranteeModel implements StreamSwitchModel {
    private static final Logger LOG = LoggerFactory.getLogger(DelayGuaranteeModel.class);
    Config config;
    StreamSwitch streamSwitch;
    class Migration{
        Map<String, String[]> partitionToMove;
        Migration(){
            partitionToMove = new HashMap<>();
        }
        void setPartitionToMove(String partition, String source, String target){
            partitionToMove.put(partition, new String[2]);
            partitionToMove.get(partition)[0] = source;
            partitionToMove.get(partition)[1] = target;
        }
        Map<String, String[]> getPartitionToMove(){
            return partitionToMove;
        }
    }
    Migration nextMigration = null;
    public DelayGuaranteeModel(Config config){
        this.config = config;
    }
    @Override
    public void init(StreamSwitch streamSwitch, ControllerListener listener, List<String> executors, List<String> partitions){
        this.streamSwitch = streamSwitch;
    }
    @Override
    public boolean updateMetrics(Map<String, Object> metrics){
        return false;
    }
    @Override
    public void migrationDeployed(){

    }
}
