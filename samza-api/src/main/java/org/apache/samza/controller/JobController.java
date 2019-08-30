package org.apache.samza.controller;

import java.util.List;

public interface JobController {
    void init(JobControllerListener listener, List<String> partitions, List<String> executors);
    void start();
    //Method used to inform Controller
    void onLastChangeImplemented();
}
