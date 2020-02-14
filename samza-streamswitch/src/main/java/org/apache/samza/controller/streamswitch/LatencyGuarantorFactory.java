package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.OperatorController;
import org.apache.samza.controller.OperatorControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyGuarantorFactory extends StreamSwitchFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyGuarantorFactory.class);

    @Override
    public StreamSwitch getController(Config config) {
        return new LatencyGuarantor(config);
    }
}
