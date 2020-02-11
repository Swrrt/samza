package org.apache.samza.controller;

import java.util.List;

public interface OperatorController {
    void init(OperatorControllerListener listener, List<String> partitions, List<String> executors);
    void start();
    //Method used to inform Controller
    void onChangeImplemented();
}
