package org.apache.samza.controller;

public interface FailureListener {
    void failureHappenOnContainer(String failedContainerId);
}
