package org.apache.samza.clustermanager;

import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.metrics.ContainerProcessManagerMetrics;
import org.apache.samza.metrics.MetricsRegistryMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScalingContainerProcessManager extends ContainerProcessManager{
    ScalingContainerProcessManager(Config config, SamzaApplicationState state, MetricsRegistryMap registry, AbstractContainerAllocator allocator, ClusterResourceManager manager) {
        super(config, state, registry, allocator, manager);
    }

    public ScalingContainerProcessManager(Config config,
                                   SamzaApplicationState state,
                                   MetricsRegistryMap registry) {
        super(config, state, registry);

    }

    //package private, used only in tests
    ScalingContainerProcessManager(Config config,
                            SamzaApplicationState state,
                            MetricsRegistryMap registry,
                            ClusterResourceManager resourceManager) {
        super(config, state, registry);
    }
}
