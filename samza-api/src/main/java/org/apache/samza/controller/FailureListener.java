package org.apache.samza.controller;

public interface FailureListener {
    void onContainerFailed(String failedContainerId);
}
