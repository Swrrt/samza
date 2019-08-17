package StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJobModelChangeStreamSwitch implements StreamSwitch{
    private static final Logger LOG = LoggerFactory.getLogger(TestJobModelChangeStreamSwitch.class);

    StreamSwitchListener listener;
    Config config;
    public TestJobModelChangeStreamSwitch(Config config){
        this.config = config;
    }
    public void init(StreamSwitchListener listener){
        this.listener = listener;
    }
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
