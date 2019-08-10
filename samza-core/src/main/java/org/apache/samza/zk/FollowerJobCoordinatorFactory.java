package org.apache.samza.zk;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerJobCoordinatorFactory implements JobCoordinatorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerJobCoordinatorFactory.class);
    private static final String JOB_COORDINATOR_ZK_PATH_FORMAT = "%s/%s-%s-coordinationData";
    private static final String DEFAULT_JOB_NAME = "defaultJob";

    /**
     * Instantiates an {@link ZkJobCoordinator} using the {@link Config}.
     *
     * @param config zookeeper configurations required for instantiating {@link ZkJobCoordinator}
     * @return An instance of {@link ZkJobCoordinator}
     */
    @Override
    public JobCoordinator getJobCoordinator(Config config) {
        // TODO: Separate JC related configs into a "ZkJobCoordinatorConfig"
        MetricsRegistry metricsRegistry = new MetricsRegistryMap();
        String jobCoordinatorZkBasePath = getJobCoordinationZkPath(config);
        ZkUtils zkUtils = getZkUtils(config, metricsRegistry, jobCoordinatorZkBasePath);
        LOG.debug("Creating ZkJobCoordinator with config: {}.", config);
        return new FollowerJobCoordinator(config, metricsRegistry, zkUtils);
    }

    private ZkUtils getZkUtils(Config config, MetricsRegistry metricsRegistry, String coordinatorZkBasePath) {
        ZkConfig zkConfig = new ZkConfig(config);
        ZkKeyBuilder keyBuilder = new ZkKeyBuilder(coordinatorZkBasePath);
        ZkClient zkClient = ZkCoordinationUtilsFactory
                .createZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
        return new ZkUtils(keyBuilder, zkClient, zkConfig.getZkConnectionTimeoutMs(), zkConfig.getZkSessionTimeoutMs(), metricsRegistry);
    }

    public static String getJobCoordinationZkPath(Config config) {
        JobConfig jobConfig = new JobConfig(config);
        String appId = new ApplicationConfig(config).getGlobalAppId();
        String jobName = jobConfig.getName().isDefined()
                ? jobConfig.getName().get()
                : DEFAULT_JOB_NAME;
        String jobId = jobConfig.getJobId();

        return String.format(JOB_COORDINATOR_ZK_PATH_FORMAT, appId, jobName, jobId);
    }
}
