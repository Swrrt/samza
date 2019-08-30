package org.apache.samza.controller;

import org.apache.samza.config.Config;

public interface JobControllerFactory {
    JobController getController(Config config);
}
