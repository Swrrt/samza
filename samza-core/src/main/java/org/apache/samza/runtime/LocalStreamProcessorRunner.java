package org.apache.samza.runtime;

import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class LocalStreamProcessorRunner {
    private static final Logger log = LoggerFactory.getLogger(LocalStreamProcessorRunner.class);
    private static volatile Throwable containerRunnerException = null;
    public static void main(String[] args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(
                new SamzaUncaughtExceptionHandler(() -> {
                    log.info("Exiting process now.");
                    System.exit(1);
                }));

        String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID());
        log.info(String.format("Got container ID: %s", containerId));
        System.out.println(String.format("Container ID: %s", containerId));

        String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
        log.info(String.format("Got coordinator URL: %s", coordinatorUrl));
        System.out.println(String.format("Coordinator URL: %s", coordinatorUrl));

        int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
        JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
        Config config = jobModel.getConfig();
        JobConfig jobConfig = new JobConfig(config);
        if (jobConfig.getName().isEmpty()) {
            throw new SamzaException("can not find the job name");
        }
        String jobName = jobConfig.getName().get();
        String jobId = jobConfig.getJobId();
        MDC.put("containerName", "samza-container-" + containerId);
        MDC.put("jobName", jobName);
        MDC.put("jobId", jobId);

        ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc =
                ApplicationDescriptorUtil.getAppDescriptor(ApplicationUtil.fromConfig(config), config);
        run(appDesc, containerId, jobModel, config);

        System.exit(0);
    }
    private static void run(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc, String containerId,
                            JobModel jobModel, Config config) {
        try {


            // create the StreamProcessors
            JobConfig jobConfig = (JobConfig)config;
            StreamProcessor processor = createStreamProcessor(jobConfig, appDesc,
                    null);
            processor.start();
        } catch (Throwable throwable) {
            throw new SamzaException(String.format("Failed to start application: %s",
                    new ApplicationConfig(appDesc.getConfig()).getGlobalAppId()), throwable);
        }
    }
    static StreamProcessor createStreamProcessor(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
                                          StreamProcessor.StreamProcessorLifecycleListenerFactory listenerFactory) {
        TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
        Map<String, MetricsReporter> reporters = new HashMap<>();
        // TODO: the null processorId has to be fixed after SAMZA-1835
        appDesc.getMetricsReporterFactories().forEach((name, factory) ->
                reporters.put(name, factory.getMetricsReporter(name, null, config)));
        return new StreamProcessor(config, reporters, taskFactory, appDesc.getApplicationContainerContextFactory(),
                appDesc.getApplicationTaskContextFactory(), listenerFactory, null);
    }

}
