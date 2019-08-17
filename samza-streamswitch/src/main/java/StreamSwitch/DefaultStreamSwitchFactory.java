package StreamSwitch;

import org.apache.samza.config.Config;
import org.apache.samza.streamswitch.StreamSwitch;
import org.apache.samza.streamswitch.StreamSwitchFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStreamSwitchFactory implements StreamSwitchFactory{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamSwitchFactory.class);

    @Override
    public StreamSwitch getStreamSwitch(Config config) {
        return new DefaultStreamSwitch(config);
    }
}
