package org.apache.samza.controller;

import java.util.List;
import java.util.Map;

public interface OperatorControllerListener {
    void remap(Map<String, List<String>> partitionAssignment);
    void scale(int parallelism, Map<String, List<String>> partitionAssignment);
}
