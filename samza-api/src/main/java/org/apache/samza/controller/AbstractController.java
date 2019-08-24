package org.apache.samza.controller;

import java.util.List;

public interface AbstractController {
    void init(ControllerListener listener, List<String> partitions, List<String> executors);
    void start();
    //Method used to inform Controller
    void lastChangeImplemented();
}
