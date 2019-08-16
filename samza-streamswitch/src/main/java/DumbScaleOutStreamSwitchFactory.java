import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbScaleOutStreamSwitchFactory implements StreamSwitchFactory{
    private static final Logger LOG = LoggerFactory.getLogger(DumbScaleOutStreamSwitchFactory.class);

    @Override
    public StreamSwitch getStreamSwitch(Config config) {
        return new DumbScaleOutStreamSwitch(config);
    }
}
