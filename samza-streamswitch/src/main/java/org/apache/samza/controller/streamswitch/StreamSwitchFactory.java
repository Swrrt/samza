package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.controller.JobController;
import org.apache.samza.controller.JobControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSwitchFactory implements JobControllerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSwitchFactory.class);

    @Override
    public JobController getController(Config config) {
        return new StreamSwitch(config);
    }
}
