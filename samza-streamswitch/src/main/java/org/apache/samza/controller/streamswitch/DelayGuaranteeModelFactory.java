package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayGuaranteeModelFactory implements StreamSwitchModelFactory{
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.samza.controller.streamswitch.DelayGuaranteeModelFactory.class);
    @Override
    public StreamSwitchModel getModel(Config config) {
        return new DelayGuaranteeModel(config);
    }
}
