package StreamSwitch;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        for(String executor: executors){
            partitionAssignment.put(executor, new LinkedList<>());
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

            }catch (Exception e){

            }

        }
    }
}
