package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.controller.JobControllerListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

//Under development

public class DelayGuaranteeStreamSwitch extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(DelayGuaranteeStreamSwitch.class);
    class NetworkCalculusModel{
        private class PartitionState{
            Map<Long, Long> arrived, completed;
            Map<Long, HashMap<String, Long>> backlog;
            PartitionState(){
                arrived = new HashMap<>();
                completed = new HashMap<>();
                backlog = new HashMap<>();
            }
        }
        private class ExecutorState{
            Map<Long, Long> completed;
            public ExecutorState(){
                completed = new HashMap<>();
            }
        }
        Map<String, PartitionState> partitionStates;
        Map<String, ExecutorState> executorStates;
        List<Long> timePoints;
        public NetworkCalculusModel(){
            partitionStates = new HashMap<>();
            executorStates = new HashMap<>();
            timePoints = new ArrayList<>();
        }
        protected List<Long> getTimePoints(){
            return timePoints;
        }
        private long getLastTime(long time){
            long lastTime = 0;
            for(int i = timePoints.size() - 1; i>=0; i--)
                if(timePoints.get(i) <= time){
                    lastTime = timePoints.get(i);
                    break;
                }
            return lastTime;
        }
        public void updatePartitionArrived(String partitionId, long time, long arrived){
            partitionStates.putIfAbsent(partitionId, new PartitionState());
            partitionStates.get(partitionId).arrived.put(time, arrived);
        }
        public void updatePartitionCompleted(String partitionId, long time, long completed){
            partitionStates.putIfAbsent(partitionId, new PartitionState());
            partitionStates.get(partitionId).completed.put(time, completed);
        }
        public void updatePartitionBacklog(String partitionId, long time, String executorId, long backlog){
            partitionStates.putIfAbsent(partitionId, new PartitionState());
            partitionStates.get(partitionId).backlog.putIfAbsent(time, new HashMap<>());
            partitionStates.get(partitionId).backlog.get(time).put(executorId, backlog);
        }
        public void updateExecutorCompleted(String executorId, long time, long completed){
            executorStates.putIfAbsent(executorId, new ExecutorState());
            executorStates.get(executorId).completed.put(time, completed);
        }
        public long getExecutorCompleted(String executorId, long time){
            long completed = 0;
            if(executorStates.containsKey(executorId) && executorStates.get(executorId).completed.containsKey(time)){
                completed = executorStates.get(executorId).completed.get(time);
            }
            return completed;
        }
        //Use last
        public long getPartitionArrived(String partitionId, long time){
            long arrived = 0;
            if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).arrived.containsKey(time)){
                arrived = partitionStates.get(partitionId).arrived.get(time);
            }
            return arrived;
        }
        public Map<String, Long> getPartitionsArrived(long time){
            HashMap<String, Long> arrived = new HashMap<>();
            for(String id: partitionStates.keySet()){
                arrived.put(id, getPartitionArrived(id, time));
            }
            return arrived;
        }
        public Map<String, Long> getPartitionsCompleted(long time){
            HashMap<String, Long> completed = new HashMap<>();
            for(String id: partitionStates.keySet()){
                completed.put(id, getPartitionCompleted(id, time));
            }
            return completed;
        }
        public long getPartitionCompleted(String partitionId, long time){
            long completed = 0;
            if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).completed.containsKey(time)){
                completed = partitionStates.get(partitionId).completed.get(time);
            }
            return completed;
        }
        public long getPartitionBacklog(String partitionId, long time, String executorId){
            long backlog = 0;
            if(partitionStates.containsKey(partitionId) && partitionStates.get(partitionId).backlog.containsKey(time)){
                backlog = partitionStates.get(partitionId).backlog.get(time).getOrDefault(executorId, 0l);
            }
            return backlog;
        }
        public long getExecutorArrived(String executorId, long time){
            long arrived = getExecutorCompleted(executorId, time);
            for(String id:partitionStates.keySet()){
                arrived += getPartitionBacklog(id, time, executorId);
            }
            return arrived;
        }
        public Map<String, Long> getExecutorsArrived(long time){
            HashMap<String, Long> arrived = new HashMap<>();
            for(String executorId: executorStates.keySet()){
                arrived.put(executorId, getExecutorArrived(executorId, time));
            }
            return arrived;
        }
        public Map<String, Long> getExecutorsCompleted(long time){
            HashMap<String, Long> completed = new HashMap<>();
            for(String executorId: executorStates.keySet()){
                completed.put(executorId, getExecutorCompleted(executorId, time));
            }
            return completed;
        }
        public void updateAtTime(long time, Map<String, Long> taskArrived, Map<String, Long> taskProcessed, Map<String, List<String>> partitionAssignment) { //Normal update
            timePoints.add(time);
            for (String executorId : partitionAssignment.keySet()) {
                long d_completed = 0;
                for (String id : partitionAssignment.get(executorId)) {
                    long arrived = taskArrived.getOrDefault(id, -1l);
                    long processed = taskProcessed.getOrDefault(id, -1l);
                    long lastArrived = 0;
                    if(timePoints.size() > 1) lastArrived = getPartitionArrived(id, timePoints.get(timePoints.size() - 2));
                    if(arrived < lastArrived) arrived = lastArrived;
                    updatePartitionArrived(id, time, arrived);
                    long lastProcessed = 0;
                    if(timePoints.size() > 1) lastProcessed = getPartitionCompleted(id, timePoints.get(timePoints.size() - 2));
                    if(processed < lastProcessed) processed = lastProcessed;
                    updatePartitionCompleted(id, time, processed);
                    //Update partition backlog
                    long backlog = 0;
                    if (timePoints.size() > 1) {
                        long lastTime = timePoints.get(timePoints.size() - 2);
                        backlog = getPartitionBacklog(id, lastTime, executorId);
                        backlog -= getPartitionArrived(id, lastTime);
                        backlog += getPartitionCompleted(id, lastTime);
                        d_completed -= getPartitionCompleted(id, lastTime);
                    }
                    backlog += arrived - processed;
                    d_completed += processed;
                    updatePartitionBacklog(id, time, executorId, backlog);
                }
                if (timePoints.size() > 1) {
                    long lastTime = timePoints.get(timePoints.size() - 2);
                    d_completed += getExecutorCompleted(executorId, lastTime);
                }
                updateExecutorCompleted(executorId, time, d_completed);
            }
        }
        public double findArrivedTime(String executorId, long completed){
            long lastTime = 0;
            long lastArrived = 0;
            if(completed == 0)return 0;
            for(int i = timePoints.size() - 1; i>=0; i--){
                long time = timePoints.get(i);
                long arrived = getExecutorArrived(executorId, time);
                if(arrived <= completed){
                    if(arrived == completed)return time;
                    return lastTime - (lastArrived - completed) *  (double)(lastTime - time) / (double)(lastArrived - arrived) ;
                }
                lastTime = time;
                lastArrived = arrived;
            }
            return -1;
        }

        public double estimateDelay(String executorId, long time, long lastTime){
            double delay = 0;
            long size = 0;
            long tTime, tLastTime = 0;
            int startPoint = timePoints.size() - 1;
            while(startPoint > 0){
                if(timePoints.get(startPoint) < lastTime)break;
                startPoint--;
            }
            for(int i = startPoint; i < timePoints.size(); i++){
                tTime = timePoints.get(i);
                if(tTime > time){
                    break;
                }
                if(tTime >= lastTime){
                    long completed = getExecutorCompleted(executorId, tTime);
                    long lastCompleted = getExecutorCompleted(executorId, tLastTime);
                    double estimateArrive = findArrivedTime(executorId, completed);
                    delay += (completed - lastCompleted) * (tTime - estimateArrive);
                    size += completed - lastCompleted;
                    //writeLog("For container " + executorId + ", estimated arrive time for completed " + completed + "(at time " + tTime + " is: " + estimateArrive + ", size is: " + (completed - lastCompleted));
                }
                tLastTime = tTime;
            }
            if(size <= 0)return -1; //No processed :(
            if(size > 0) delay /= size;
            if(delay < 1e-10) delay = 0;
            return delay;
        }
        public void migration(long time, String srcExecutorId, String tgtExecutorId, String partionId){
            for(int i = timePoints.size() - 1; i >= 0;i--){
                if(time >= timePoints.get(i)){
                    time = timePoints.get(i);
                    break;
                }
            }

            long backlog = getPartitionBacklog(partionId, time, srcExecutorId);
            long arrived = getPartitionArrived(partionId, time);
            for(int i = timePoints.size() - 1 ; i >=0 ; i--){
                long tTime = timePoints.get(i);
                long tArrived = getPartitionArrived(partionId, tTime);
                if(tArrived < arrived - backlog){
                    break;
                }
                long sBacklog = getPartitionBacklog(partionId, tTime, srcExecutorId);
                long tBacklog = getPartitionBacklog(partionId, tTime, tgtExecutorId);
                updatePartitionBacklog(partionId, tTime, srcExecutorId, sBacklog - (tArrived - (arrived - backlog)));
                updatePartitionBacklog(partionId, tTime, tgtExecutorId, tBacklog + (tArrived - (arrived - backlog)));
            }
        }
        public void showExecutors(String label){
            for(String id: executorStates.keySet()){
                showExecutor(id, label);
            }
        }
        public void showExecutor(String executorId, String label){
            HashMap<String, Long> backlog = new HashMap<>();
            writeLog("DelayEstimator, show executor " + executorId + " " + label);
            for(int i=0;i<timePoints.size();i++){
                long time = timePoints.get(i);
                backlog.clear();
                for(int partition = 0; partition < partitionStates.keySet().size(); partition ++){
                    String id = "Partition " + partition;
                    backlog.put(String.valueOf(partition), getPartitionBacklog(id, time, executorId));
                }
                writeLog("DelayEstimator, time: " + time + " Arrived: " + getExecutorArrived(executorId, time) + " Completed: " + getExecutorCompleted(executorId, time) + " Backlog: " + backlog);
            }
            writeLog("DelayEstimator, end of executor " + executorId);
        }

        private void writeLog(String string){
            System.out.println("DelayEstimator: " + string);
        }
    }

    class DelayEstimateModel{
        private class PartitionData{
            Map<Long, Double> arrivalRate;
            PartitionData(){
                arrivalRate = new HashMap<>();
            }
        }
        private class ExecutorData{
            Map<Long, Double> arrivalRate;
            Map<Long, Double> serviceRate;
            Map<Long, Double> avgDelay;
            Map<Long, Double> avgResidual;
            Map<Long, Double> utilization;
            ExecutorData(){
                arrivalRate = new HashMap<>();
                serviceRate = new HashMap<>();
                avgDelay = new HashMap<>();
                avgResidual = new HashMap<>();
                utilization = new HashMap<>();
            }
        }
        private Map<String, ExecutorData> executors;
        private Map<String, PartitionData> partitions;
        private List<Long> times;
        private NetworkCalculusModel networkCalculusModel;
        private Map<String, Deque<Pair<Long, Double>>> delayWindows;
        private int alpha = 1, beta = 2;
        private long interval = 0;
        public DelayEstimateModel(){
            times = new ArrayList<>();
            executors = new HashMap<>();
            partitions = new HashMap<>();
            delayWindows = new HashMap<>();
        }
        public void setTimes(long interval, int a, int b){
            this.interval = interval;
            alpha = a;
            beta = b;
        }
        public void setTimes(List<Long> times){
            this.times = times;
        }
        public long getCurrentTime(){
            if(times.size() == 0)return 0;
            return times.get(times.size() - 1);
        }
        public void setNetworkCalculusModel(NetworkCalculusModel networkCalculusModel){
            this.networkCalculusModel = networkCalculusModel;
        }

        // 1 / ( u - n ). Return  1e100 if u <= n
        public double getLongTermDelay(String executorId, long time){
            double arrival = getExecutorArrivalRate(executorId, time);
            double service = getExecutorServiceRate(executorId, time);
            if(service < arrival + 1e-15)return 1e100;
            return 1.0/(service - arrival);
        }

        public double getExecutorArrivalRate(String executorId, long time){
            return executors.get(executorId).arrivalRate.getOrDefault(time, 0.0);
        }
        public double getExecutorServiceRate(String executorId, long time) {
            return executors.get(executorId).serviceRate.getOrDefault(time, 0.0);
        }
        public double getAvgDelay(String executorId, long time){
            return executors.get(executorId).avgDelay.getOrDefault(time, 0.0);
        }
        public double getAvgResidual(String executorId, long time){
            return executors.get(executorId).avgResidual.getOrDefault(time, 0.0);
        }
        public double getUtilization(String executorId, long time){
            return executors.get(executorId).utilization.getOrDefault(time, 0.0);
        }
        public double getUtilization(String executorId, long time, long lastTime){
            double sum = 0;
            int numberOfInterval = 0;
            for(int i = times.size() - 1; i>=0; i--){
                long tTime = times.get(i);
                if(tTime < lastTime)break;
                if(tTime <= time){
                    numberOfInterval ++;
                    sum += getUtilization(executorId, tTime);
                }
            }
            if(numberOfInterval == 0)return 0;
            else return sum/numberOfInterval;
        }
        public double getPartitionArriveRate(String paritionId, long time){
            return partitions.get(paritionId).arrivalRate.getOrDefault(time, 0.0);
        }
        public void updatePartitionArriveRate(String partitionId, long time, double value){
            if(!partitions.containsKey(partitionId)){
                partitions.put(partitionId, new PartitionData());
            }
            partitions.get(partitionId).arrivalRate.put(time, value);
        }
        public void updateExecutorArriveRate(String executorId, long time, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).arrivalRate.put(time, value);
        }
        public void updateExecutorServiceRate(String executorId, long time, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).serviceRate.put(time, value);
        }
        public void updateExecutorUtilization(String executorId, long time, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).utilization.put(time, value);
        }
        public void updateAvgDelay(String executorId, long time, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).avgDelay.put(time, value);
        }
        public void updateAvgResidual(String executorId, long time, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).avgResidual.put(time, value);
        }
        public long getLastTime(long time){
            long lastTime = 0;
            for(int i = times.size() - 1; i>=0;i--){
                lastTime = times.get(i);
                if(lastTime < time)break;
            }
            return lastTime;
        }
        public void updateAtTime(long time, Map<String, Double> containerUtilization, Map<String, List<String>> partitionAssignment){
            for(Map.Entry<String, List<String>> entry: partitionAssignment.entrySet()) {
                String containerId = entry.getKey();
                double s_arrivalRate = 0;
                long lastTime = getLastTime(time - beta * interval);
                for (String partitionId  : entry.getValue()) {
                    long arrived = networkCalculusModel.getPartitionArrived(partitionId, time);
                    long lastArrived = networkCalculusModel.getPartitionArrived(partitionId, lastTime);
                    double arrivalRate = 0;
                    if(time > lastTime) arrivalRate = (arrived - lastArrived) / ((double) time - lastTime);
                    updatePartitionArriveRate(partitionId, time, arrivalRate);
                    s_arrivalRate += arrivalRate;
                }
                updateExecutorArriveRate(containerId, time, s_arrivalRate);

                //Update actual service rate (capability)
                long completed = networkCalculusModel.getExecutorCompleted(containerId, time);
                long lastCompleted = networkCalculusModel.getExecutorCompleted(containerId, lastTime);
                double util = containerUtilization.getOrDefault(containerId, 1.0);
                updateExecutorUtilization(containerId, time, util);
                util = getUtilization(containerId, time, lastTime);
                if(util < 1e-10){
                    //TODO: change this
                    util = 1;
                }
                double serviceRate = 0;
                if(time > lastTime) serviceRate = (completed - lastCompleted)/(((double)time - lastTime) * util);
                updateExecutorServiceRate(containerId, time, serviceRate);

                //Update avg delay
                double delay = networkCalculusModel.estimateDelay(containerId, time, time);
                if(!delayWindows.containsKey(containerId)){
                    delayWindows.put(containerId, new LinkedList<>());
                }
                Deque<Pair<Long, Double>> window = delayWindows.get(containerId);
                if(delay > -1e-9) window.addLast(new Pair(time, delay)); //Only if it has processed
                while(window.size() > 0 && time - window.getFirst().getKey() > alpha * interval){
                    window.pollFirst();
                }
                Iterator<Pair<Long, Double>> iterator = window.iterator();
                double s_Delay = 0;
                while(iterator.hasNext()){
                    s_Delay += iterator.next().getValue();
                }
                double avgDelay = 0;
                if(window.size() > 0)avgDelay = s_Delay / window.size();
                updateAvgDelay(containerId, time, avgDelay);

                //Update residual
                lastTime = getLastTime(time - interval);
                double avgResidual = getAvgResidual(containerId, lastTime);
                double rho = s_arrivalRate / serviceRate;
                double queueDelay = (avgDelay - 1 / serviceRate);
                if(queueDelay > 1e-9 && rho < 1 && rho > 1e-9){
                    avgResidual = queueDelay * (1 - rho) / rho;
                }
                updateAvgResidual(containerId, time, avgResidual);
            }
        }
        public void showData(){
            LOG.info("Show delay estimation data...");
            LOG.info("Partition arrival rate:");
        }
    }

    NetworkCalculusModel networkCalculusModel;
    DelayEstimateModel delayEstimateModel;
    long migrationWarmupTime, migrationInterval, lastTime;
    AtomicLong nextExecutorID;
    double instantaneousThreshold, longTermThreshold;

    public DelayGuaranteeStreamSwitch(Config config){
        super(config);

        migrationWarmupTime = config.getLong("streamswitch.migration.warmup.time");
        migrationInterval = config.getLong("streamswitch.migration.interval.time");
        instantaneousThreshold = config.getDouble("streamswitch.delay.instant.threshold", 100.0);
        longTermThreshold = config.getDouble("streamswtich.delay.longterm.threshold", 100.0);

        lastTime = -1000000000l;

        networkCalculusModel = new NetworkCalculusModel();
        delayEstimateModel = new DelayEstimateModel();
        delayEstimateModel.setNetworkCalculusModel(networkCalculusModel);
    }

    /* Algorithm: Iterate through pairs of executors, and search through all possible set of partitions,
        to find the minimal expected delay way to migrate.
        TODO

    */
    private static final String MIGRATION_FAIL= "FAIL", MIGRATION_SUCCEED = "SUCCEED", MIGRATION_NEEDSCALEOUT = "NEED_SCALE_OUT";

    class MigrationResult{
        Map<String, Pair<String,String>> migratingPartitions;
        String resultCode;
        MigrationResult(){
            migratingPartitions = null;
            resultCode = MIGRATION_FAIL;
        }
        MigrationResult(String code, Map<String, Pair<String, String>> migratingPartitions){
            resultCode = code;
            this.migratingPartitions = migratingPartitions;
        }
    }
    private MigrationResult tryToMigrate(){
        LOG.info("Try to migrate");


        return new MigrationResult();
    }

    //Return false if both instantaneous and long-term thresholds are violated
    public boolean checkDelay(String containerId){
        double delay = delayEstimateModel.getAvgDelay(containerId, delayEstimateModel.getCurrentTime());
        double longTermDelay = delayEstimateModel.getLongTermDelay(containerId, delayEstimateModel.getCurrentTime());
        if(delay > instantaneousThreshold && longTermDelay > longTermThreshold){
            return false;
        }
        return true;
    }

    private long getNextExecutorID(){
        return nextExecutorID.get();
    }

    private void setNextExecutorId(long id){
        if(id > nextExecutorID.get()){
            nextExecutorID.set(id);
        }
    }

    private Pair<String, Double> findMaxInstantaneousDelay(Map<String, List<String>> partitionAssignment, long time){
        double initialDelay = -1.0;
        String maxExecutor = "";
        for (String executor : partitionAssignment.keySet()) {
            double delay = delayEstimateModel.getAvgDelay(executor, time);
            if (delay > initialDelay && !checkDelay(executor)) {
                initialDelay = delay;
                maxExecutor = executor;
            }
        }
        return new Pair(maxExecutor, initialDelay);
    }

    private Pair<String, Double> findMaxLongtermDelayExecutor(Map<String, List<String>> partitionAssignment, long time){
        double initialDelay = -1.0;
        String maxExecutor = "";
        for (String executor : partitionAssignment.keySet()) {
            double longtermDelay = delayEstimateModel.getLongTermDelay(executor, time);
            if (longtermDelay > initialDelay && !checkDelay(executor)) {
                initialDelay = longtermDelay;
                maxExecutor = executor;
            }
        }
        return new Pair(maxExecutor, initialDelay);
    }

    private MigrationResult tryToScaleOut(){
        LOG.info("Scale out by one container");

        if(partitionAssignment.size() <= 0){
            LOG.info("No executor to move");
            return new MigrationResult();
        }
        long time = delayEstimateModel.getCurrentTime();
        Pair<String, Double> a = findMaxLongtermDelayExecutor(partitionAssignment, time);
        String srcExecutor = a.getKey();
        double initialDelay = a.getValue();
        if(srcExecutor == null || srcExecutor.equals("") || partitionAssignment.get(srcExecutor).size() <=1){
            LOG.info("Cannot scale out: insufficient partition to migrate");
            return new MigrationResult();
        }

        Map<String, Pair<String, String>> migratingPartitions = new HashMap<>();
        long newExecutorId = getNextExecutorID();
        String tgtExecutor = String.format("%06d", newExecutorId);
        int numToMigrate = partitionAssignment.get(srcExecutor).size()/2;
        for(String partition: partitionAssignment.get(srcExecutor)){
            if(numToMigrate > 0){
                migratingPartitions.put(partition, new Pair(srcExecutor, tgtExecutor));
                numToMigrate--;
            }
        }
        setNextExecutorId(newExecutorId + 1);
        return new MigrationResult("Succeed", migratingPartitions);
    }

    private double estimateLongtermDelay(double arrivalRate, double serviceRate) {
        if(serviceRate < arrivalRate + 1e-15)return 1e100;
        return 1.0/(serviceRate - arrivalRate);
    }
    //TODO
    private MigrationResult tryToScaleIn(){
        LOG.info("Try to scale in");
        long time = delayEstimateModel.getCurrentTime();
        if(partitionAssignment.size() <= 1){
            LOG.info("Not enough executor to merge");
            return new MigrationResult();
        }
        for(String src: partitionAssignment.keySet()){
            double srcArrival = delayEstimateModel.getExecutorArrivalRate(src, time);
            for(String tgt: partitionAssignment.keySet())
                if(!src.equals(tgt)){
                    double tgtArrival = delayEstimateModel.getExecutorArrivalRate(tgt, time);
                    double tgtService = delayEstimateModel.getExecutorServiceRate(tgt, time);
                    double tgtInstantDelay = delayEstimateModel.getAvgDelay(tgt, time);
                    if(tgtInstantDelay < instantaneousThreshold && srcArrival + tgtArrival < tgtService){
                        double estimatedLongtermDelay = estimateLongtermDelay(srcArrival + tgtArrival, tgtService);
                        //Scale In
                        if(estimatedLongtermDelay < longTermThreshold){
                            Map<String, Pair<String, String>> migratingPartitions = new HashMap<>();
                            for(String partition: partitionAssignment.get(src)){
                                migratingPartitions.put(partition, new Pair<>(src, tgt));
                            }
                            LOG.info("Scale in! from " + src + " to " + tgt);
                            LOG.info("Migrating partitions: " + migratingPartitions.keySet());
                            return new MigrationResult(MIGRATION_SUCCEED, migratingPartitions);
                        }
                    }
                }
        }
        LOG.info("Cannot find any scale in");
        return new MigrationResult();
    }

    MigrationResult lastResult;
    Map<String, List<String>> generatePartitionAssignmentWhenMigrating(Map<String, List<String>> oldAssignment, Map<String, Pair<String, String>> migratingPartitions){
        Map<String, List<String>> newAssignment = new HashMap<>();
        for(String executor: oldAssignment.keySet()){
            for(String partition: oldAssignment.get(executor)){
                if(!migratingPartitions.containsKey(partition)){
                    if(!newAssignment.containsKey(executor))newAssignment.put(executor, new LinkedList<>());
                    newAssignment.get(executor).add(partition);
                }else{
                    String tgt = migratingPartitions.get(partition).getValue();
                    if(!newAssignment.containsKey(tgt))newAssignment.put(tgt, new LinkedList<>());
                    newAssignment.get(tgt).add(partition);
                }
            }
        }
        return newAssignment;
    }
    Map<String, Long> lastProcessCPUtime, lastProcessTime;
    private double processCPUtimeToUtilization(String executorId, long processCPUtime, long time){
        double value = -1.0;
        if(lastProcessCPUtime == null){
            lastProcessCPUtime = new HashMap<>();
            lastProcessTime = new HashMap<>();
        }
        if(lastProcessCPUtime.containsKey(executorId)){
            value = (processCPUtime - lastProcessCPUtime.get(executorId))/1000000.0 / (time - lastProcessTime.get(executorId));
        }
        lastProcessCPUtime.put(executorId, processCPUtime);
        lastProcessTime.put(executorId, time);
        return value;
    }

    @Override
    protected boolean updateModel(long time, Map<String, Object> metrics){
        LOG.info("Updating model from metrics");


        Map<String, Long> partitionArrived =
                (HashMap<String, Long>)(metrics.get("PartitionArrived"));
        Map<String, Long> partitionProcessed =
                (HashMap<String, Long>)(metrics.get("PartitionProcessed"));

        //Translate processCPUtime to utilization
        Map<String, Double> executorUtilization = new HashMap<>();
        Map<String, Long> times =
                (HashMap<String, Long>)(metrics.get("Time"));
        for(Map.Entry<String, Long> entry: ((HashMap<String, Long>)(metrics.get("ProcessCPUTime"))).entrySet()){
            executorUtilization.put(entry.getKey(), processCPUtimeToUtilization(entry.getKey(), entry.getValue(), times.get(entry.getKey())));
        }

        updateNetworkCalculus(time, partitionArrived, partitionProcessed);
        updateDelayEstimateModel(time, executorUtilization);

        //Map<String, String> containerArrived = (HashMap<String, String>)(metrics.get(""))
        if(waitForMigrationDeployed){
            LOG.info("Last migration is not deployed, cannot migrate");
        }else if(time - startTime <= migrationWarmupTime){
            LOG.info("Still in warmup phase, cannot migrate");
        }else if(time - lastTime <= migrationInterval) {
            LOG.info("To close to last migration decision, cannot migrate");
        }else{
            //TODO: check delay and migrate
            LOG.info("Check delay guarantee is followed");
            if(!checkDelayGuarantee()){
                //TODO: try to migrate
                MigrationResult result = tryToMigrate();
                if(result.resultCode.equals(MIGRATION_SUCCEED)){
                    LOG.info("OK to migrate");
                    lastResult = result;
                    Map<String, List<String>> newAssignment = generatePartitionAssignmentWhenMigrating(partitionAssignment, result.migratingPartitions);
                    listener.changePartitionAssignment(newAssignment);
                    waitForMigrationDeployed = true;
                }else if(result.resultCode.equals(MIGRATION_NEEDSCALEOUT)){
                    //TODO: try to scaling
                    result = tryToScaleOut();
                    if(result.resultCode.equals(MIGRATION_SUCCEED)){
                        LOG.info("OK to scale out!");
                        lastResult = result;
                        Map<String, List<String>> newAssignment = generatePartitionAssignmentWhenMigrating(partitionAssignment, result.migratingPartitions);
                        listener.scaling(newAssignment.size(), newAssignment);
                        waitForMigrationDeployed = true;
                    }
                }else{
                    LOG.info("Error, some thing wrong with migration algorithm");
                }
            }else{
                LOG.info("Delay guarantee is not violated, try to scaleIn");
                MigrationResult result = tryToScaleIn();
                if(result.equals(MIGRATION_SUCCEED)){
                    LOG.info("OK to scale in");
                    lastResult = result;
                    Map<String, List<String>> newAssignment = generatePartitionAssignmentWhenMigrating(partitionAssignment, result.migratingPartitions);
                    listener.scaling(newAssignment.size(), newAssignment);
                    waitForMigrationDeployed = true;
                }else{
                    LOG.info("Cannot scale in, do nothing");
                }
            }
        }
        return false;
    };

    private boolean checkDelayGuarantee(String executorId){
        double delay = delayEstimateModel.getAvgDelay(executorId, delayEstimateModel.getCurrentTime());
        double longTermDelay = delayEstimateModel.getLongTermDelay(executorId, delayEstimateModel.getCurrentTime());
        if(delay > instantaneousThreshold && longTermDelay > longTermThreshold){
            return false;
        }
        return true;
    }
    //True if delay guarantee is not violated.
    private boolean checkDelayGuarantee(){
        List<String> decreasingExecutors = new ArrayList<>();
        for(String executorId: partitionAssignment.keySet()){
            double delay = delayEstimateModel.getAvgDelay(executorId, delayEstimateModel.getCurrentTime());
            double arrival = delayEstimateModel.getExecutorArrivalRate(executorId, delayEstimateModel.getCurrentTime());
            double service = delayEstimateModel.getExecutorServiceRate(executorId, delayEstimateModel.getCurrentTime());
            double longtermDelay = delayEstimateModel.getLongTermDelay(executorId, delayEstimateModel.getCurrentTime());
            if(!checkDelayGuarantee(executorId)){
                System.out.println("Executor " + executorId
                        + " instant delay is " + delay + " exceeds threshold: " + instantaneousThreshold
                        + " longterm delay is " + longtermDelay + " exceeds threshold: " + longTermThreshold
                        + ", arrival is " + arrival + ", service is " + service);
                return false;
            }else if(delay > instantaneousThreshold){
                decreasingExecutors.add(executorId);
            }
        }
        if(decreasingExecutors.size()==0)System.out.println("All containers' delay is smaller than threshold");
        else System.out.println("Containers delay is greater than threshold, but estimated to decrease: " + decreasingExecutors);

        return true;
    }

    private void updateNetworkCalculus(long time, Map<String, Long> partitionArrived, Map<String, Long> partitionProcessed){
        LOG.info("Updating network calculus model...");
        networkCalculusModel.updateAtTime(time, partitionArrived, partitionProcessed, partitionAssignment);

        //Debug & Statistics
        if(true){
            HashMap<String, Double> delays = new HashMap<>();
            for(String executorId: partitionAssignment.keySet()){
                double delay = networkCalculusModel.estimateDelay(executorId, time, time);
                delays.put(executorId, delay);
            }
            System.out.println("Network Calculus Model, time " + time + " , Arrived: " + networkCalculusModel.getExecutorsArrived(time));
            System.out.println("Network Calculus Model, time " + time + " , Processed: " + networkCalculusModel.getExecutorsCompleted(time));
            System.out.println("Network Calculus Model, time " + time + " , Delay: " + delays);
            System.out.println("Network Calculus Model, time " + time + " , Partition Arrived: " + networkCalculusModel.getPartitionsArrived(time));
            System.out.println("Network Calculus Model, time " + time + " , Partition Processed: " + networkCalculusModel.getPartitionsCompleted(time));
        }
    }

    private void updateDelayEstimateModel(long time, Map<String, Double> executorUtilization){
        LOG.info("Updating Delay Estimating model");
        delayEstimateModel.updateAtTime(time, executorUtilization, partitionAssignment);

        //Debug & Statistics
        if(true){
            HashMap<String, Double> arrivalRate = new HashMap<>();
            HashMap<String, Double> serviceRate = new HashMap<>();
            HashMap<String, Double> avgDelay = new HashMap<>();
            HashMap<String, Double> longtermDelay = new HashMap<>();
            HashMap<String, Double> residual = new HashMap<>();
            HashMap<String, Double> partitionArrivalRate = new HashMap<>();
            HashSet<String> partitions = new HashSet<>();
            for(String executorId: partitionAssignment.keySet()){
                double arrivalR = delayEstimateModel.getExecutorArrivalRate(executorId, time);
                arrivalRate.put(executorId, arrivalR);
                double serviceR = delayEstimateModel.getExecutorServiceRate(executorId, time);
                serviceRate.put(executorId, serviceR);
                double delay = delayEstimateModel.getAvgDelay(executorId, time);
                avgDelay.put(executorId, delay);
                delay = delayEstimateModel.getLongTermDelay(executorId, time);
                longtermDelay.put(executorId, delay);
                double res = delayEstimateModel.getAvgResidual(executorId, time);
                residual.put(executorId, res);
                partitions.addAll(partitionAssignment.get(executorId));
            }
            System.out.println("DelayEstimateModel, time " + time + " : " + "Arrival Rate: " + arrivalRate);
            System.out.println("DelayEstimateModel, time " + time + " : " + "Service Rate: " + serviceRate);
            System.out.println("DelayEstimateModel, time " + time + " : " + "Average Delay: " + avgDelay);
            System.out.println("DelayEstimateModel, time " + time + " : " + "Longterm Delay: " + longtermDelay);
            System.out.println("DelayEstimateModel, time " + time + " : " + "Residual: " + residual);
            for(String partitionId: partitions){
                double arrivalR = delayEstimateModel.getPartitionArriveRate(partitionId, time);
                partitionArrivalRate.put(partitionId, arrivalR);
            }
            System.out.println("MixedLoadBalanceManager, time " + time + " : " + "Partition Arrival Rate: " + partitionArrivalRate);
        }
    }

    //TODO
    @Override
    public synchronized void onChangeImplemented(){
        LOG.info("Migration actually deployed");
        if(waitForMigrationDeployed){
            LOG.info("Update mapping and models");
            waitForMigrationDeployed = false;
            partitionAssignment = generatePartitionAssignmentWhenMigrating(partitionAssignment, lastResult.migratingPartitions);
            long time = System.currentTimeMillis();
            for(String partition: lastResult.migratingPartitions.keySet()){
                networkCalculusModel.migration(time, lastResult.migratingPartitions.get(partition).getKey(), lastResult.migratingPartitions.get(partition).getValue(), partition);
            }
            lastResult = new MigrationResult();
        }
    }

    public void showData(){
        LOG.info("Show data:");
        networkCalculusModel.showExecutors("");
        delayEstimateModel.showData();

    }
}
