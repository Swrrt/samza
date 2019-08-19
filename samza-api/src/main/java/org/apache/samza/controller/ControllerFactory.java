package org.apache.samza.controller;

import org.apache.samza.config.Config;

public interface ControllerFactory {
    AbstractController getController(Config config);
}
