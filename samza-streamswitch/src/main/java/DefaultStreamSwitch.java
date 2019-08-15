import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchListener;

public class DefaultStreamSwitch implements StreamSwitch{
    StreamSwitchListener listener;
    public void init(StreamSwitchListener listener){
        this.listener = listener;
    }
    public void start(){
        while(true){
            tryToScale();
        }
    }
    void tryToScale(){
        int startNumber = 1;
        for(int i=startNumber;i<10;i++) {
            try{
                Thread.sleep(10000);
            }catch(Exception e){
            }
        }
    }
}
