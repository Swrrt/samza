package org.apache.samza.clustermanager;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.*;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.util.Util;
import org.apache.samza.zk.LeaderJobCoordinator;
import org.apache.samza.zk.LeaderJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class YarnApplicationMaster extends ClusterBasedJobCoordinator{
    private static final Logger log = LoggerFactory.getLogger(YarnApplicationMaster.class);

    private final LeaderJobCoordinator leaderJobCoordinator;

    public YarnApplicationMaster(Config coordinatorSystemConfig){
        super(coordinatorSystemConfig);
        leaderJobCoordinator = createLeaderJobCoordinator(coordinatorSystemConfig);
    }
    @Override
    public void run(){
        super.run();
        startLeader();
    }
    private LeaderJobCoordinator createLeaderJobCoordinator(Config config) {
        String jobCoordinatorFactoryClassName = "LeaderJobCoordinator";
        return (LeaderJobCoordinator)Util.getObj(jobCoordinatorFactoryClassName, LeaderJobCoordinatorFactory.class).getJobCoordinator(config);
    }
    private void startLeader(){
        leaderJobCoordinator.start();
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
}
