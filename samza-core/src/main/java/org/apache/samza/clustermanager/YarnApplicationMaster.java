/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.PartitionChangeException;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.*;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.stream.CoordinatorStreamManager;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.controller.OperatorController;
import org.apache.samza.controller.OperatorControllerFactory;
import org.apache.samza.controller.OperatorControllerListener;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.apache.samza.zk.LeaderJobCoordinator;
import org.apache.samza.zk.LeaderJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class YarnApplicationMaster {
    private static final Logger log = LoggerFactory.getLogger(YarnApplicationMaster.class);

    private final Config config;
    private final ClusterManagerConfig clusterManagerConfig;

    /**
     * State to track container failures, host-container mappings
     */
    private final SamzaApplicationState state;

    //even though some of these can be converted to local variables, it will not be the case
    //as we add more methods to the JobCoordinator and completely implement SAMZA-881.

    /**
     * Handles callback for allocated containers, failed containers.
     */
    private final ContainerProcessManager containerProcessManager;

    /**
     * A JobModelManager to return and refresh the {@link org.apache.samza.job.model.JobModel} when required.
     */
    private final JobModelManager jobModelManager;

    /**
     * A ChangelogStreamManager to handle creation of changelog stream and map changelog stream partitions
     */
    private final ChangelogStreamManager changelogStreamManager;

    /**
     * Single instance of the coordinator stream to use.
     */
    private final CoordinatorStreamManager coordinatorStreamManager;

    /*
     * The interval for polling the Task Manager for shutdown.
     */
    private final long jobCoordinatorSleepInterval;

    /*
     * Config specifies if a Jmx server should be started on this Job Coordinator
     */
    private final boolean isJmxEnabled;

    /**
     * Internal boolean to check if the job coordinator has already been started.
     */
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * A boolean variable indicating whether the job has durable state stores in the configuration
     */
    private final boolean hasDurableStores;

    /**
     * The input topic partition count monitor
     */
    private final StreamPartitionCountMonitor partitionMonitor;

    /**
     * Metrics to track stats around container failures, needed containers etc.
     */
    private final MetricsRegistryMap metrics;

    /**
     * Internal variable for the instance of {@link JmxServer}
     */
    private JmxServer jmxServer;

    /**
     * Variable to keep the callback exception
     */
    volatile private Exception coordinatorException = null;

    private SystemAdmins systemAdmins = null;

    /**
     * Creates a new ClusterBasedJobCoordinator instance from a config. Invoke run() to actually
     * run the jobcoordinator.
     *
     * @param coordinatorSystemConfig the coordinator stream config that can be used to read the
     *                                {@link org.apache.samza.job.model.JobModel} from.
     */
    public YarnApplicationMaster(Config coordinatorSystemConfig) {

        metrics = new MetricsRegistryMap();

        coordinatorStreamManager = new CoordinatorStreamManager(coordinatorSystemConfig, metrics);
        // register ClusterBasedJobCoordinator with the CoordinatorStreamManager.
        coordinatorStreamManager.register(getClass().getSimpleName());
        // start the coordinator stream's underlying consumer and producer.
        coordinatorStreamManager.start();
        // bootstrap current configuration.
        coordinatorStreamManager.bootstrap();

        // build a JobModelManager and ChangelogStreamManager and perform partition assignments.
        changelogStreamManager = new ChangelogStreamManager(coordinatorStreamManager);
        jobModelManager = JobModelManager.apply(coordinatorStreamManager.getConfig(), changelogStreamManager.readPartitionMapping());

        config = jobModelManager.jobModel().getConfig();
        hasDurableStores = new StorageConfig(config).hasDurableStores();
        state = new SamzaApplicationState(jobModelManager);
        // The systemAdmins should be started before partitionMonitor can be used. And it should be stopped when this coordinator is stopped.
        systemAdmins = new SystemAdmins(config);
        partitionMonitor = getPartitionCountMonitor(config, systemAdmins);
        clusterManagerConfig = new ClusterManagerConfig(config);
        isJmxEnabled = clusterManagerConfig.getJmxEnabledOnJobCoordinator();

        jobCoordinatorSleepInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();

        // build a container process Manager
        containerProcessManager = new ContainerProcessManager(config, state, metrics);

        controller = createController();

        numOfContainers = (new JobConfig(config)).getContainerCount();

    }

    private OperatorController createController(){
        String controllerFactoryClassName = config.getOrDefault("job.controller.factory", "org.apache.samza.controller.DefaultJobControllerFactory");
        return Util.getObj(controllerFactoryClassName, OperatorControllerFactory.class).getController(config);
    }

    /**
     * Starts the JobCoordinator.
     *
     */
    public void run() {
        if (!isStarted.compareAndSet(false, true)) {
            log.info("Attempting to start an already started job coordinator. ");
            return;
        }
        // set up JmxServer (if jmx is enabled)
        if (isJmxEnabled) {
            jmxServer = new JmxServer();
            state.jmxUrl = jmxServer.getJmxUrl();
            state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
        } else {
            jmxServer = null;
        }

        try {
            //initialize JobCoordinator state
            log.info("Starting Cluster Based Job Coordinator");

            //create necessary checkpoint and changelog streams, if not created
            JobModel jobModel = jobModelManager.jobModel();
            oldJobModel = jobModelManager.jobModel();
            CheckpointManager checkpointManager = new TaskConfigJava(config).getCheckpointManager(metrics);
            if (checkpointManager != null) {
                checkpointManager.createResources();
            }
            ChangelogStreamManager.createChangelogStreams(jobModel.getConfig(), jobModel.maxChangeLogStreamPartitions);

            // Remap changelog partitions to tasks
            Map<TaskName, Integer> prevPartitionMappings = changelogStreamManager.readPartitionMapping();

            Map<TaskName, Integer> taskPartitionMappings = new HashMap<>();
            Map<String, ContainerModel> containers = jobModel.getContainers();
            for (ContainerModel containerModel: containers.values()) {
                for (TaskModel taskModel : containerModel.getTasks().values()) {
                    taskPartitionMappings.put(taskModel.getTaskName(), taskModel.getChangelogPartition().getPartitionId());
                }
            }

            changelogStreamManager.updatePartitionMapping(prevPartitionMappings, taskPartitionMappings);

            containerProcessManager.start();
            systemAdmins.start();
            partitionMonitor.start();

            //Start leader
            startLeader();

            //Start JobController
            startController(containers);

            boolean isInterrupted = false;
            while (!containerProcessManager.shouldShutdown() && !checkAndThrowException() && !isInterrupted) {
                try {
                    Thread.sleep(jobCoordinatorSleepInterval);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                    log.error("Interrupted in job coordinator loop {} ", e);
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Throwable e) {
            log.error("Exception thrown in the JobCoordinator loop {} ", e);
            throw new SamzaException(e);
        } finally {
            onShutDown();
        }
    }

    private boolean checkAndThrowException() throws Exception {
        if (coordinatorException != null) {
            throw coordinatorException;
        }
        return false;
    }

    /**
     * Stops all components of the JobCoordinator.
     */
    private void onShutDown() {

        try {
            partitionMonitor.stop();
            systemAdmins.stop();
            containerProcessManager.stop();
            coordinatorStreamManager.stop();
        } catch (Throwable e) {
            log.error("Exception while stopping task manager {}", e);
        }
        log.info("Stopped task manager");

        if (jmxServer != null) {
            try {
                jmxServer.stop();
                log.info("Stopped Jmx Server");
            } catch (Throwable e) {
                log.error("Exception while stopping jmx server {}", e);
            }
        }
    }

    private StreamPartitionCountMonitor getPartitionCountMonitor(Config config, SystemAdmins systemAdmins) {
        StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
        Set<SystemStream> inputStreamsToMonitor = new TaskConfigJava(config).getAllInputStreams();
        if (inputStreamsToMonitor.isEmpty()) {
            throw new SamzaException("Input streams to a job can not be empty.");
        }

        return new StreamPartitionCountMonitor(
                inputStreamsToMonitor,
                streamMetadata,
                metrics,
                new JobConfig(config).getMonitorPartitionChangeFrequency(),
                streamsChanged -> {
                    // Fail the jobs with durable state store. Otherwise, application state.status remains UNDEFINED s.t. YARN job will be restarted
                    if (hasDurableStores) {
                        log.error("Input topic partition count changed in a job with durable state. Failing the job.");
                        state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
                    }
                    coordinatorException = new PartitionChangeException("Input topic partition count changes detected.");
                });
    }

    // The following two methods are package-private and for testing only
    @VisibleForTesting
    SamzaApplicationState.SamzaAppStatus getAppStatus() {
        // make sure to only return a unmodifiable copy of the status variable
        final SamzaApplicationState.SamzaAppStatus copy = state.status;
        return copy;
    }

    @VisibleForTesting
    StreamPartitionCountMonitor getPartitionMonitor() {
        return partitionMonitor;
    }

    public static void main(String[] args) {
        Config coordinatorSystemConfig = null;
        final String coordinatorSystemEnv = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG());
        try {
            //Read and parse the coordinator system config.
            log.info("Parsing coordinator system config {}", coordinatorSystemEnv);
            coordinatorSystemConfig = new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(coordinatorSystemEnv, Config.class));
        } catch (IOException e) {
            log.error("Exception while reading coordinator stream config {}", e);
            throw new SamzaException(e);
        }
        YarnApplicationMaster am = new YarnApplicationMaster(coordinatorSystemConfig);
        am.run();
    }


    //
    // Methods and variables we add
    private LeaderJobCoordinator leaderJobCoordinator = null;

    private OperatorController controller = null;

    private int numOfContainers;

    private JobModel oldJobModel;

    private JobModel generateJobModelFromPartitionAssignment(Map<String, List<String>> partitionAssignment){
        //Ignore locality manager?
        Map<String, TaskModel> taskModels = new HashMap<>();
        for(ContainerModel containerModel: oldJobModel.getContainers().values()){
            for(Map.Entry<TaskName, TaskModel> entry: containerModel.getTasks().entrySet()){
                taskModels.put(entry.getKey().getTaskName(), entry.getValue());
            }
        }
        Map<String, ContainerModel> containers = new HashMap<>();
        for(String containerId: partitionAssignment.keySet()){
            Map<TaskName, TaskModel> tasks = new HashMap<>();
            for(String taskId: partitionAssignment.get(containerId)){
                tasks.put(new TaskName(taskId), taskModels.get(taskId));
            }
            containers.put(containerId, new ContainerModel(containerId, tasks));
        }

        return new JobModel(config, containers);
    }

    private LeaderJobCoordinator createLeaderJobCoordinator(Config config) {
        String jobCoordinatorFactoryClassName = LeaderJobCoordinatorFactory.class.getName();
        return (LeaderJobCoordinator)Util.getObj(jobCoordinatorFactoryClassName, LeaderJobCoordinatorFactory.class).getJobCoordinator(config);
    }
    private void startLeader(){
        //long migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000l);
        leaderJobCoordinator = createLeaderJobCoordinator(config);
        leaderJobCoordinator.start();
        leaderJobCoordinator.setListener(new JobCoordinatorListener() {
            @Override
            public void onJobModelExpired() {
            }

            @Override
            public void onNewJobModel(String processorId, JobModel jobModel) {
                log.info("New Job Model actually deployed!");
                controller.onMigrationExecutorsStopped();
                //TODO: add thread to wait for 5s and call
                /*log.info("Start a thread wait for migration interval then call migration completed");
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try{
                            log.info("Sleep for "+ migrationInterval + " then call migration completed");
                            Thread.sleep(migrationInterval);
                            controller.onMigrationCompleted();
                        }catch (Exception e){
                            log.warn("Exception when sleep for migration interval");
                            //Still need to call migration completed
                            controller.onMigrationCompleted();
                        }
                    }
                }).start();*/
            }

            @Override
            public void onCoordinatorStop() {

            }

            @Override
            public void onCoordinatorFailure(Throwable t) {

            }
        });
    }

    private void startController(Map<String, ContainerModel> containers){
        List<String> container, tasks;
        container = new LinkedList<>();
        tasks = new LinkedList<>();
        int count = 0;
        for(Map.Entry<String, ContainerModel> centry: containers.entrySet()){
            if(centry.getKey().length() < 6)
                container.add(String.format("%06d", Integer.parseInt(centry.getKey()) + 2));
            else container.add(centry.getKey());
            for(Map.Entry<TaskName, TaskModel> entry: centry.getValue().getTasks().entrySet()){
                tasks.add(entry.getKey().getTaskName());
            }
        }
        //log.info("Current jobModel is : " + jobModelManager.jobModel());
        controller.init(new OperatorControllerListener() {
            @Override
            public void remap(Map<String, List<String>> partitionAssignment) {
                log.info("Receive request to change partitionAssignment");
                JobModel newJobModel = generateJobModelFromPartitionAssignment(partitionAssignment);
                log.info("New JobModel = " + newJobModel);
                if(newJobModel == null){
                    log.info("No partition-executor mapping is given, use auto-generated JobModel instead");
                }else leaderJobCoordinator.setNewJobModel(newJobModel);
            }

            @Override
            public void scale(int parallelism, Map<String, List<String>> partitionAssignment) {
                log.info("Try to change number of executors to: " + parallelism + " from " + numOfContainers);
                if(partitionAssignment == null){
                    log.info("No partition-executor mapping is given, break out");
                    return ;
                }
                JobModel newJobModel = generateJobModelFromPartitionAssignment(partitionAssignment);
                log.info("parallelism: " + parallelism + " old: " + numOfContainers);
                log.info("New JobModel = " + newJobModel);
                if(numOfContainers < parallelism){   //Scale out
                    log.info("Scale out!");
                    int numToScaleOut = parallelism - numOfContainers;
                    if(newJobModel != null)leaderJobCoordinator.setNewJobModel(newJobModel);
                    for(int i=0;i<numToScaleOut;i++)containerProcessManager.scaleOut();
                    numOfContainers = parallelism;
                }else if(numOfContainers > parallelism){  //Scale in
                    log.info("Scale in!");
                    if(newJobModel != null)leaderJobCoordinator.setNewJobModel(newJobModel);
                    numOfContainers = parallelism;
                }
            }
        }, container, tasks);
        controller.start();
    }
}
