import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbScaleOutStreamSwitch implements StreamSwitch{
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutStreamSwitch.class);

    StreamSwitchListener listener;
    Config config;
    public DumbScaleOutStreamSwitch(Config config){
        this.config = config;
    }
    public void init(StreamSwitchListener listener){
        this.listener = listener;
    }
    public void start(){
        LOG.info("Start stream switch");
        while(true){
            tryToScale();
        }
    }
    void tryToScale(){
        int startNumber = 1;
        for(int i=startNumber+1;i<=10;i++) {
            try{
                Thread.sleep(30000);
                LOG.info("Try to scale out");
                listener.scaling(i, null);
            }catch(Exception e){
            }
        }
    }
}
