package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.samza.config.Config;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

//Under development

public class DelayGuaranteeStreamSwitch extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(DelayGuaranteeStreamSwitch.class);
    class State {
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
        boolean allValid;
        long windowSize;
        public State(){
            partitionStates = new HashMap<>();
            executorStates = new HashMap<>();
            timePoints = new ArrayList<>();
            allValid = false;
            windowSize = 100000;
        }
        public void setWindowSize(long size){
            windowSize = size;
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

        /*
            Remove state that older than lastTime
         */
        public void popOldState(long lastTime){
            LinkedList<Long> removedTime = new LinkedList<>();
            for(long time: timePoints) {
                if(time < lastTime){
                    removedTime.add(time);
                    for(Map.Entry<String, PartitionState> entry: partitionStates.entrySet()){
                        PartitionState state = entry.getValue();
                        if(state.arrived.containsKey(time))state.arrived.remove(time);
                        if(state.backlog.containsKey(time))state.backlog.remove(time);
                        if(state.completed.containsKey(time))state.completed.remove(time);
                    }
                    for(Map.Entry<String, ExecutorState> entry: executorStates.entrySet()){
                        ExecutorState state = entry.getValue();
                        if(state.completed.containsKey(time))state.completed.remove(time);
                    }
                }
            }
            for(long time: removedTime){
                timePoints.remove(time);
            }
        }
        /*
            Replace invalid data in state with valid estimation
            when current snapshot is all valid
         */
        public void calibrate(long time){
            //Calibrate partition state
            for(String partitionId: partitionStates.keySet()){
            }
            //Calibrate executorId
            allValid = true;
        }

        public void updateAtTime(long time, Map<String, Long> taskArrived, Map<String, Long> taskProcessed, Map<String, List<String>> partitionAssignment) { //Normal update
            LOG.info("Debugging, time: " + time + " taskArrived: "+ taskArrived + " taskProcessed: "+ taskProcessed + " assignment: " + partitionAssignment);
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
                //LOG.info("Debugging, executor " + executorId + " dcompleted: " + d_completed);
                updateExecutorCompleted(executorId, time, d_completed);
            }
            popOldState(time - windowSize - 1);
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
    private class ExamineResult{
        private Prescription pres;
        private List<Pair<String, Double>> longtermDelay;
        ExamineResult(Prescription pres, List<Pair<String, Double>> longtermDelay){
            this.pres = pres;
            this.longtermDelay = longtermDelay;
        }
        protected Prescription getPres(){
            return pres;
        }
        protected List<Pair<String, Double>> getLongtermDelay(){
            return longtermDelay;
        }
    }
    class Model {
        private class PartitionData{
            double arrivalRate;
            PartitionData(){
                arrivalRate = -1.0;
            }
        }
        private class ExecutorData{
            double arrivalRate;
            double serviceRate;
            double avgDelay;
            Deque<Pair<Long, Double>> utilization;
            ExecutorData(){
                arrivalRate = -1.0;
                serviceRate = -1.0;
                avgDelay = -1.0;
                utilization = new LinkedList<>();
            }
        }
        private Map<String, ExecutorData> executors;
        private Map<String, PartitionData> partitions;
        private long lastTime;
        private State state;
        private Map<String, Deque<Pair<Long, Double>>> delayWindows;
        private int alpha = 1, beta = 2;
        private long interval = 0;
        public Model(){
            lastTime = -1;
            executors = new HashMap<>();
            partitions = new HashMap<>();
            delayWindows = new HashMap<>();
        }
        public void setTimes(long interval, int a, int b){
            this.interval = interval;
            alpha = a;
            beta = b;
        }
        public long getCurrentTime(){
            return lastTime;
        }
        public void setState(State state){
            this.state = state;
        }

        // 1 / ( u - n ). Return  1e100 if u <= n
        public double getLongTermDelay(String executorId){
            double arrival = getExecutorArrivalRate(executorId);
            double service = getExecutorServiceRate(executorId);
            if(service < arrival + 1e-15)return 1e100;
            return 1.0/(service - arrival);
        }

        public double getExecutorArrivalRate(String executorId){
            return executors.get(executorId).arrivalRate;
        }
        public double getExecutorServiceRate(String executorId) {
            return executors.get(executorId).serviceRate;
        }
        public double getAvgDelay(String executorId){
            return executors.get(executorId).avgDelay;
        }
        public double getUtilization(String executorId){
            return executors.get(executorId).utilization.getLast().getValue();
        }
        public double getWindowedUtilization(String executorId, long time, long lastTime){
            double sum = 0;
            int numberOfInterval = 0;
            for(Iterator<Pair<Long,Double>> i = executors.get(executorId).utilization.iterator();i.hasNext();){
                Pair<Long, Double> pair = i.next();
                if(pair.getKey() <= time){
                    sum += pair.getValue();
                    numberOfInterval++;
                    if(pair.getKey() < lastTime)break;
                }
            }
            if(numberOfInterval == 0)return 0;
            else return sum/numberOfInterval;
        }
        public double getPartitionArriveRate(String paritionId){
            return partitions.get(paritionId).arrivalRate;
        }
        public void updatePartitionArriveRate(String partitionId, double value){
            if(!partitions.containsKey(partitionId)){
                partitions.put(partitionId, new PartitionData());
            }
            partitions.get(partitionId).arrivalRate = value;
        }
        public void updateExecutorArriveRate(String executorId, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).arrivalRate = value;
        }
        public void updateExecutorServiceRate(String executorId, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).serviceRate = value;
        }
        public void updateExecutorUtilization(String executorId, long time, long lastTime, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).utilization.addLast(new Pair(time, value));
            while(executors.get(executorId).utilization.getFirst().getKey() < lastTime){
                executors.get(executorId).utilization.pollFirst();
            }
        }
        public void updateAvgDelay(String executorId, double value){
            if(!executors.containsKey(executorId)){
                executors.put(executorId, new ExecutorData());
            }
            executors.get(executorId).avgDelay = value;
        }
        public long getLastTime(long time){
            return state.getLastTime(time);
        }
        public void updateAtTime(long time, Map<String, Double> containerUtilization, Map<String, List<String>> partitionAssignment){
            for(Map.Entry<String, List<String>> entry: partitionAssignment.entrySet()) {
                String containerId = entry.getKey();
                double s_arrivalRate = 0;
                long lastTime = getLastTime(time - beta * interval);
                for (String partitionId  : entry.getValue()) {
                    long arrived = state.getPartitionArrived(partitionId, time);
                    long lastArrived = state.getPartitionArrived(partitionId, lastTime);
                    double arrivalRate = 0;
                    if(time > lastTime) arrivalRate = (arrived - lastArrived) / ((double) time - lastTime);
                    //LOG.info("Debugging,  time: " + time + " last time: " + lastTime + " arrived: " + arrived + "lastArrived: " + lastArrived + " arrivalRate: " + arrivalRate);
                    updatePartitionArriveRate(partitionId, arrivalRate);
                    s_arrivalRate += arrivalRate;
                }
                //LOG.info("Debugging,  time: " + time + " last time: " + lastTime + " s_arrivalRate: " + s_arrivalRate);
                updateExecutorArriveRate(containerId, s_arrivalRate);

                //Update actual service rate (capability)
                long completed = state.getExecutorCompleted(containerId, time);
                long lastCompleted = state.getExecutorCompleted(containerId, lastTime);
                double util = containerUtilization.getOrDefault(containerId, 1.0);
                updateExecutorUtilization(containerId, time, lastTime, util);
                util = getWindowedUtilization(containerId, time, lastTime);
                if(util < 1e-10){
                    //TODO: change this
                    util = 1;
                }
                double serviceRate = 0;
                if(time > lastTime) serviceRate = (completed - lastCompleted)/(((double)time - lastTime) * util);
                updateExecutorServiceRate(containerId, serviceRate);

                //Update avg delay
                double delay = state.estimateDelay(containerId, time, time);
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
                updateAvgDelay(containerId, avgDelay);
            }
        }
        public void showData(){
            LOG.info("Show delay estimation data...");
            LOG.info("Partition arrival rate:");
        }
    }

    State state;
    Model model;
    long migrationWarmupTime, migrationInterval, lastTime;
    double instantaneousThreshold, longTermThreshold;

    public DelayGuaranteeStreamSwitch(Config config){
        super(config);

        migrationWarmupTime = config.getLong("streamswitch.migration.warmup.time", 1000000000l);
        migrationInterval = config.getLong("streamswitch.migration.interval.time", 1000l);
        instantaneousThreshold = config.getDouble("streamswitch.delay.instant.threshold", 100.0);
        longTermThreshold = config.getDouble("streamswtich.delay.longterm.threshold", 100.0);

        lastTime = -1000000000l;
        state = new State();
        model = new Model();
        model.setState(state);
        model.setTimes(config.getLong("streamswitch.delay.interval", 500l), config.getInt("streamswitch.delay.alpha", 20), config.getInt("streamswitch.delay.beta", 10));
        algorithms = new Strategies();
        updateLock = new ReentrantLock();
    }

    class Prescription {
        String source, target;
        List<String> migratingPartitions;
        Prescription(){
            migratingPartitions = null;
        }
        Prescription(String source, String target, List<String> migratingPartitions){
            this.source = source;
            this.target = target;
            this.migratingPartitions = migratingPartitions;
        }
    }


    //Algorithms packed
    protected class Strategies {
        public ExamineResult tryToScaleOut(){
            LOG.info("Scale out by one container");

            if(partitionAssignment.size() <= 0){
                LOG.info("No executor to move");
                return new ExamineResult(new Prescription(), null);
            }
            long time = model.getCurrentTime();
            Pair<String, Double> a = findMaxLongtermDelayExecutor(partitionAssignment, time);
            String srcExecutor = a.getKey();
            double initialDelay = a.getValue();
            if(srcExecutor == null || srcExecutor.equals("") || partitionAssignment.get(srcExecutor).size() <=1){
                LOG.info("Cannot scale out: insufficient partition to migrate");
                return new ExamineResult(new Prescription(), null);
            }

            List<String> migratingPartitions = new ArrayList<>();
            long newExecutorId = getNextExecutorID();
            String tgtExecutor = String.format("%06d", newExecutorId);
            int numToMigrate = partitionAssignment.get(srcExecutor).size()/2;
            for(String partition: partitionAssignment.get(srcExecutor)){
                if(numToMigrate > 0){
                    migratingPartitions.add(partition);
                    numToMigrate--;
                }
            }
            setNextExecutorId(newExecutorId + 1);
            LOG.info("Debugging, scaling out migrating partitions: " + migratingPartitions);
            return new ExamineResult(new Prescription(srcExecutor, tgtExecutor, migratingPartitions), null); //TODO: add here
        }
        public ExamineResult tryToScaleIn(){
            LOG.info("Try to scale in");
            long time = model.getCurrentTime();
            if(partitionAssignment.size() <= 1){
                LOG.info("Not enough executor to merge");
                return new ExamineResult(new Prescription(), null);
            }
            String minsrc = "", mintgt = "";
            double minLongtermDelay = -1;
            for(String src: partitionAssignment.keySet()){
                double srcArrival = model.getExecutorArrivalRate(src);
                for(String tgt: partitionAssignment.keySet())
                    if(!src.equals(tgt)){
                        double tgtArrival = model.getExecutorArrivalRate(tgt);
                        double tgtService = model.getExecutorServiceRate(tgt);
                        double tgtInstantDelay = model.getAvgDelay(tgt);
                        if(tgtInstantDelay < instantaneousThreshold && srcArrival + tgtArrival < tgtService){
                            double estimatedLongtermDelay = estimateLongtermDelay(srcArrival + tgtArrival, tgtService);
                            //Scale In
                            if(estimatedLongtermDelay < longTermThreshold && (minLongtermDelay < -1e-9 || estimatedLongtermDelay < minLongtermDelay)){
                                minLongtermDelay = estimatedLongtermDelay;
                                minsrc = src;
                                mintgt = tgt;
                                /*
                                Map<String, Pair<String, String>> migratingPartitions = new HashMap<>();
                                for(String partition: partitionAssignment.get(src)){
                                    migratingPartitions.put(partition, new Pair<>(src, tgt));
                                }
                                LOG.info("Scale in! from " + src + " to " + tgt);
                                LOG.info("Migrating partitions: " + migratingPartitions.keySet());
                                return new Prescription(MIGRATION_SUCCEED, migratingPartitions);
                                */
                            }
                        }
                    }
            }
            if(minLongtermDelay > -1e-9){
                List<String> migratingPartitions = new ArrayList<>(partitionAssignment.get(minsrc));
                LOG.info("Scale in! from " + minsrc + " to " + mintgt);
                LOG.info("Migrating partitions: " + migratingPartitions);
                return new ExamineResult(new Prescription(minsrc, mintgt, migratingPartitions), null); //TODO: add here
            }
            LOG.info("Cannot find any scale in");
            return new ExamineResult(new Prescription(), null);
        }

        /*

            Followings are used for re-balance algorithm

         */
        private class DFSState {
            String srcContainer, tgtContainer;
            double srcArrivalRate, tgtArrivalRate, srcServiceRate, tgtServiceRate;
            long time;
            List<String> srcPartitions;
            List<String> tgtPartitions;
            Set<String> migratingPartitions;
            double bestDelay;
            Set<String> bestMigration;
            String bestSrcContainer, bestTgtContainer;

            DFSState() {
                bestDelay = 1e100;
                migratingPartitions = new HashSet<>();
                bestMigration = new HashSet<>();
            }

            protected boolean okToMigratePartition(String partition) {
                double partitionArrivalRate = model.getPartitionArriveRate(partition);
                return (partitionArrivalRate + tgtArrivalRate < tgtServiceRate - 1e-12);

            }

            protected void migratingPartition(String partition) {
                migratingPartitions.add(partition);
                double arrivalRate = model.getPartitionArriveRate(partition);
                srcArrivalRate -= arrivalRate;
                tgtArrivalRate += arrivalRate;
            }

            protected void unmigratingPartition(String partition) {
                migratingPartitions.remove(partition);
                double arrivalRate = model.getPartitionArriveRate(partition);
                srcArrivalRate += arrivalRate;
                tgtArrivalRate -= arrivalRate;
            }
        }

        private Pair<String, Double> findIdealLongtermContainer(DFSState dfsState, String srcContainer, Map<String, List<String>> containerTasks, long time) {
            double minIdealDelay = 1e100;
            String tgtContainer = "";
            for (String container : containerTasks.keySet()) {
                if (container.equals(srcContainer)) continue;
                double n1 = dfsState.srcArrivalRate;
                double n2 = model.getExecutorArrivalRate(container);
                double u1 = dfsState.srcServiceRate;
                double u2 = model.getExecutorServiceRate(container);
                double instantDelay = model.getAvgDelay(container);
                LOG.info("Debugging, try to move to executor " + container + ", (a1, a2, u1, u2) are: " + n1 + ", " + n2 + ", " + u1 + ", " + u2 + ", instant delay: " + instantDelay);
                if(instantDelay < instantaneousThreshold && u2 > n2 && u2 - n2 > u1 - n1){
                    double x = ((u2 - n2) - (u1 - n1))/2;
                    if(u2 > n2 + x && u1 > n1 - x){
                        double d1 = 1/(u2 - (n2 + x));
                        double d2 = 1/(u1 - (n1 - x));
                        LOG.info("Estimate ideal long term delay: " + d1 + " , " + d2);
                        if(d1 < minIdealDelay){
                            minIdealDelay = d1;
                            tgtContainer = container;
                        }
                    }
                }
            }
            return new Pair(tgtContainer, minIdealDelay);
        }

        public double estimateLongtermDelay(double arrivalRate, double serviceRate) {
            if(serviceRate < arrivalRate + 1e-15)return 1e100;
            return 1.0/(serviceRate - arrivalRate);
        }

        private double estimateSrcLongtermDelay(DFSState state) {
            return estimateLongtermDelay(state.srcArrivalRate, state.srcServiceRate);
        }

        private double estimateTgtLongtermDelay(DFSState state) {
            return estimateLongtermDelay(state.tgtArrivalRate, state.tgtServiceRate);
        }


        private void DFSforBestLongtermDelay(int i, DFSState state) {

            if (state.srcArrivalRate > 1e-12 && state.srcArrivalRate < state.srcServiceRate && state.tgtArrivalRate < state.tgtServiceRate) { //Cannot move all partitions out
                double estimateSrc = estimateSrcLongtermDelay(state), estimateTgt = estimateTgtLongtermDelay(state);
                LOG.info("If migrating partitions " + state.migratingPartitions
                        + " from " + state.srcContainer
                        + " to " + state.tgtContainer
                        + ", estimate source delay: " + estimateSrc
                        + ", estimate target delay: " + estimateTgt
                        + ", current best delay: " + state.bestDelay
                        + ", srcArrivalRate: " + state.srcArrivalRate
                        + ", tgtArrivalRate: " + state.tgtArrivalRate
                        + ", srcServiceRate: " + state.srcServiceRate
                        + ", tgtServiceRate: " + state.tgtServiceRate
                );
                if (estimateTgt > estimateSrc && estimateSrc > state.bestDelay) return;
                if (estimateSrc < state.bestDelay && estimateTgt < state.bestDelay) {
                    state.bestDelay = Math.max(estimateSrc, estimateTgt);
                    state.bestMigration.clear();
                    state.bestMigration.addAll(state.migratingPartitions);
                    state.bestTgtContainer = state.tgtContainer;
                    state.bestSrcContainer = state.srcContainer;
                }
            }
            if (i < 0) {
                return;
            }

            //String partitionId = state.srcPartitions.get(i);

            for (int j = i - 1; j >= 0; j--) {
                String partitionId = state.srcPartitions.get(j);
                if (state.okToMigratePartition(partitionId)) { //Migrate j
                    state.migratingPartition(partitionId);
                    DFSforBestLongtermDelay(j, state);
                    state.unmigratingPartition(partitionId);
                }
            }
        }

        private Pair<String, Double> findMaxLongtermDelay(Map<String, List<String>> containerTasks, long time){
            double initialDelay = -1.0;
            String maxContainer = "";
            for (String containerId : containerTasks.keySet()) {
                double delay = model.getLongTermDelay(containerId);
                if (delay > initialDelay && !checkDelay(containerId)) {
                    initialDelay = delay;
                    maxContainer = containerId;
                }
            }
            return new Pair(maxContainer, initialDelay);
        }

        //TODO: implement DFS
        public ExamineResult tryToMigrate(){
            LOG.info("Try to migrate");
            LOG.info("Migrating once based on assignment: " + partitionAssignment);
            Map<String, List<String>> containerTasks = new HashMap<>();
            long time = model.getCurrentTime();
            containerTasks = partitionAssignment;
            if (containerTasks.keySet().size() == 0) { //No executor to move
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }
            DFSState dfsState = new DFSState();
            dfsState.time = time;

            //Find container with maximum delay
            Pair<String, Double> a = findMaxLongtermDelay(containerTasks, time);
            String srcContainer = a.getKey();
            double initialDelay = a.getValue();
            if (srcContainer.equals("")) { //No correct container
                LOG.info("Cannot find the container that exceeds threshold");
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }

            if (containerTasks.get(srcContainer).size() <= 1) { //Container has only one partition
                LOG.info("Largest delay container " + srcContainer + " has only " + containerTasks.get(srcContainer).size());
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }
            LOG.info("Try to migrate from largest delay container " + srcContainer);
            dfsState.bestDelay = initialDelay;
            dfsState.bestSrcContainer = srcContainer;
            dfsState.bestTgtContainer = srcContainer;
            dfsState.bestMigration.clear();
            //Migrating this container
            dfsState.srcContainer = srcContainer;
            dfsState.srcArrivalRate = model.getExecutorArrivalRate(srcContainer);
            dfsState.srcServiceRate = model.getExecutorServiceRate(srcContainer);
            dfsState.srcPartitions = containerTasks.get(srcContainer);
            //Choose target container based on ideal delay (minimize ideal delay)
            a = findIdealLongtermContainer(dfsState, srcContainer, containerTasks, time);
            String tgtContainer = a.getKey();
            double minIdealDelay = a.getValue();
            if(tgtContainer.equals("")){
                LOG.info("Cannot find available migration");
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }
            LOG.info("Find minimal ideal container " + tgtContainer + " , ideal delay: " + minIdealDelay);
/*        for (String tgtContainer : containerTasks.keySet())
            if (!srcContainer.equals(tgtContainer)) {*/
            double tgtArrivalRate = model.getExecutorArrivalRate(tgtContainer);
            double tgtServiceRate = model.getExecutorServiceRate(tgtContainer);
            if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                int srcSize = containerTasks.get(srcContainer).size();
                dfsState.tgtPartitions = containerTasks.get(tgtContainer);
                dfsState.tgtArrivalRate = tgtArrivalRate;
                dfsState.tgtServiceRate = tgtServiceRate;
                dfsState.migratingPartitions.clear();
                dfsState.tgtContainer = tgtContainer;
                DFSforBestLongtermDelay(srcSize, dfsState);
                //Bruteforce(srcSize, dfsState);
            }
            /*            } */

            if (dfsState.bestDelay > initialDelay - 1e-9) {
                LOG.info("Cannot find any better migration");
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }

            if(dfsState.bestDelay > longTermThreshold){
                LOG.info("Cannot find migration smaller than threshold");
                ExamineResult result = new ExamineResult(new Prescription(), null);
                return result;
            }
            LOG.info("Find best migration with delay: " + dfsState.bestDelay + ", from container " + dfsState.bestSrcContainer + " to container " + dfsState.bestTgtContainer + ", partitions: " + dfsState.bestMigration);

            List<String> migratingTasks = new ArrayList<>(dfsState.bestMigration);
            PriorityQueue<Pair<String, Double>> priorityQueue = new PriorityQueue<>((x,y)-> {
                    if(x.getValue() - 1e-9 > y.getValue())return 1;
                    if(y.getValue() - 1e-9 > x.getValue())return -1;
                    return 0;
            });
            String src = dfsState.bestSrcContainer, tgt = dfsState.bestTgtContainer;
            //Calculate new longterm delay vector
            for(String executor: partitionAssignment.keySet()){
                if(executor.equals(dfsState.srcContainer)){ //Source executor
                    priorityQueue.add(new Pair<>(executor, dfsState.bestDelay));
                }else if(executor.equals(dfsState.tgtContainer)){ //Target executor
                    priorityQueue.add(new Pair<>(executor, dfsState.bestDelay));
                }else{ //Not touched executor
                    priorityQueue.add(new Pair<>(executor, examiner.model.getLongTermDelay(executor)));
                }
            }
            List<Pair<String, Double>> longtermDelay = new ArrayList<>();
            while(!priorityQueue.isEmpty()){
                longtermDelay.add(priorityQueue.poll());
            }
            ExamineResult result = new ExamineResult(new Prescription(src, tgt, migratingTasks),longtermDelay);
            return result;
        }
    }

    class Examiner{
        private Model model;
        private State state;
        private StreamSwitchMetricsRetriever metricsRetriever;
        private boolean isValid, isMigrating;
        private Prescription pendingPres;
        Examiner(StreamSwitchMetricsRetriever metricsRetriever){
            this.metricsRetriever = metricsRetriever;
            isValid = true;
        }
        private long examine(){
            long time = System.currentTimeMillis();
            Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
            Map<String, Long> partitionArrived =  (HashMap<String, Long>) (metrics.get("PartitionArrived"));
            Map<String, Long> partitionProcessed =
                    (HashMap<String, Long>) (metrics.get("PartitionProcessed"));
            Map<String, Double> executorUtilization =
                    (HashMap<String, Double>) (metrics.get("ExecutorUtilization"));
            //TODO: check valid or not here

            updateState(time, partitionArrived, partitionProcessed);
            updateDelayEstimateModel(time, executorUtilization);
            return time;
        }
        private List<Pair<String, Double>> getInstantDelay(){
            List<Pair<String, Double>> delay = new LinkedList<>();
            for(String executor: partitionAssignment.keySet()){
                delay.add(new Pair(executor,model.getAvgDelay(executor)));
            }
            return delay;
        }

        private List<Pair<String, Double>> getLongtermDelay(){
            List<Pair<String, Double>> delay = new LinkedList<>();
            for(String executor: partitionAssignment.keySet()){
                delay.add(new Pair(executor,model.getLongTermDelay(executor)));
            }
            return delay;
        }

        private ExamineResult loadBalance(){
            return algorithms.tryToMigrate();
        }
        private ExamineResult scaleIn(){
            return algorithms.tryToScaleIn();
        }
        private ExamineResult scaleOut(){
            return algorithms.tryToScaleOut();
        }
    }

    Strategies algorithms;

    //Return false if both instantaneous and long-term thresholds are violated
    public boolean checkDelay(String containerId){
        double delay = model.getAvgDelay(containerId);
        double longTermDelay = model.getLongTermDelay(containerId);
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
            double delay = model.getAvgDelay(executor);
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
            double longtermDelay = model.getLongTermDelay(executor);
            if (longtermDelay > initialDelay && !checkDelay(executor)) {
                initialDelay = longtermDelay;
                maxExecutor = executor;
            }
        }
        return new Pair(maxExecutor, initialDelay);
    }

    private double estimateLongtermDelay(double arrivalRate, double serviceRate) {
        if(serviceRate < arrivalRate + 1e-15)return 1e100;
        return 1.0/(serviceRate - arrivalRate);
    }

    Prescription lastResult;
    Map<String, List<String>> generatePartitionAssignmentFromPrescription(Map<String, List<String>> oldAssignment, Prescription pres){
        Map<String, List<String>> newAssignment = new HashMap<>(partitionAssignment);
        if (!newAssignment.containsKey(pres.target)) newAssignment.put(pres.target, new LinkedList<>());
        for (String partition : pres.migratingPartitions) {
            newAssignment.get(pres.source).remove(partition);
            newAssignment.get(pres.target).add(partition);
        }
        //For scale in
        if (newAssignment.get(pres.source).size() == 0) newAssignment.remove(pres.source);
        return newAssignment;
    }
    Map<String, Long> lastProcessCPUtime, lastProcessTime;

    private boolean checkDelayGuarantee(String executorId){
        double delay = model.getAvgDelay(executorId);
        double longTermDelay = model.getLongTermDelay(executorId);
        if(delay > instantaneousThreshold && longTermDelay > longTermThreshold){
            return false;
        }
        return true;
    }
    //True if delay guarantee is not violated.
    private boolean checkDelayGuarantee(){
        List<String> decreasingExecutors = new ArrayList<>();
        for(String executorId: partitionAssignment.keySet()){
            double delay = model.getAvgDelay(executorId);
            double arrival = model.getExecutorArrivalRate(executorId);
            double service = model.getExecutorServiceRate(executorId);
            double longtermDelay = model.getLongTermDelay(executorId);
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

    private void updateState(long time, Map<String, Long> partitionArrived, Map<String, Long> partitionProcessed){
        LOG.info("Updating network calculus model...");
        state.updateAtTime(time, partitionArrived, partitionProcessed, partitionAssignment);

        //Debug & Statistics
        if(true){
            HashMap<String, Double> delays = new HashMap<>();
            for(String executorId: partitionAssignment.keySet()){
                double delay = state.estimateDelay(executorId, time, time);
                delays.put(executorId, delay);
            }
            System.out.println("State, time " + time + " , Arrived: " + state.getExecutorsArrived(time));
            System.out.println("State, time " + time + " , Processed: " + state.getExecutorsCompleted(time));
            System.out.println("State, time " + time + " , Delay: " + delays);
            System.out.println("State, time " + time + " , Partition Arrived: " + state.getPartitionsArrived(time));
            System.out.println("State, time " + time + " , Partition Processed: " + state.getPartitionsCompleted(time));
        }
    }

    private void updateDelayEstimateModel(long time, Map<String, Double> executorUtilization){
        LOG.info("Updating Delay Estimating model");
        model.updateAtTime(time, executorUtilization, partitionAssignment);

        //Debug & Statistics
        if(true){
            HashMap<String, Double> arrivalRate = new HashMap<>();
            HashMap<String, Double> serviceRate = new HashMap<>();
            HashMap<String, Double> avgDelay = new HashMap<>();
            HashMap<String, Double> longtermDelay = new HashMap<>();
            HashMap<String, Double> partitionArrivalRate = new HashMap<>();
            HashSet<String> partitions = new HashSet<>();
            for(String executorId: partitionAssignment.keySet()){
                double arrivalR = model.getExecutorArrivalRate(executorId);
                arrivalRate.put(executorId, arrivalR);
                double serviceR = model.getExecutorServiceRate(executorId);
                serviceRate.put(executorId, serviceR);
                double delay = model.getAvgDelay(executorId);
                avgDelay.put(executorId, delay);
                delay = model.getLongTermDelay(executorId);
                longtermDelay.put(executorId, delay);
                partitions.addAll(partitionAssignment.get(executorId));
            }
            System.out.println("Model, time " + time + " : " + "Arrival Rate: " + arrivalRate);
            System.out.println("Model, time " + time + " : " + "Service Rate: " + serviceRate);
            System.out.println("Model, time " + time + " : " + "Average Delay: " + avgDelay);
            System.out.println("Model, time " + time + " : " + "Longterm Delay: " + longtermDelay);
            for(String partitionId: partitions){
                double arrivalR = model.getPartitionArriveRate(partitionId);
                partitionArrivalRate.put(partitionId, arrivalR);
            }
            System.out.println("Model, time " + time + " : " + "Partition Arrival Rate: " + partitionArrivalRate);
        }
    }

    //We do not use this method anymore in paper's version
    @Override
    protected boolean updateModel(long time, Map<String, Object> metrics) {
        return false;
    }
    //Check healthisness of input delay vectors: 0 for Good, 1 for Moderate, 2 for Severe
    private int checkHealthiness(List<Pair<String, Double>> instantDelay, List<Pair<String, Double>> longtermDelay){
        int instantExceeded = 0;
        int longtermExceeded = 0;
        for(Pair<String, Double> entry: instantDelay){
            if(entry.getValue() > instantaneousThreshold)instantExceeded = 1;
        }
        for(Pair<String, Double> entry: longtermDelay){
            if(entry.getValue() > longTermThreshold)longtermExceeded = 1;
        }
        return instantExceeded + longtermExceeded;
    }

    private Prescription diagnose(long time, Examiner examiner){
        int healthiness = checkHealthiness(examiner.getInstantDelay(), examiner.getLongtermDelay());
        Prescription pres = new Prescription(null, null, null);

        if(examiner.isMigrating){
            LOG.info("Migration does not complete, cannot diagnose");
            return pres;
        }

        //Moderate
        if(healthiness == 1){
            LOG.info("Current healthiness is Moderate, do nothing");
            return pres;
        }

        //Good
        if(healthiness == 0){
            //Try scale in
            ExamineResult result = examiner.scaleIn();
            int thealthiness = checkHealthiness(examiner.getInstantDelay(), result.getLongtermDelay());
            if(thealthiness == 0){  //Scale in OK
                return result.getPres();
            }else{
                //Do nothing
                return pres;
            }
        }
        //Severe
        else{
            ExamineResult result = examiner.loadBalance();
            int thealthiness = checkHealthiness(examiner.getInstantDelay(), result.getLongtermDelay());
            if(thealthiness == 1){  //Load balance OK
                return result.getPres();
            }else{
                //Scale out
                result = examiner.scaleOut();
                return result.getPres();
            }
        }
    }
    //Treatment for Samza
    private void doTreatment(Prescription pres){
        if(pres.migratingPartitions == null){
            Log.warn("Prescription has nothing, so do no treatment");
            return ;
        }

        examiner.pendingPres = pres;
        examiner.isMigrating = true;

        Map<String, List<String>> newAssignment = new HashMap<>(partitionAssignment);
        //For scale out
        if (!newAssignment.containsKey(pres.target)) newAssignment.put(pres.target, new LinkedList<>());
        for (String partition : pres.migratingPartitions) {
            newAssignment.get(pres.source).remove(partition);
            newAssignment.get(pres.target).add(partition);
        }
        //For scale in
        if (newAssignment.get(pres.source).size() == 0) newAssignment.remove(pres.source);

        //Scale out
        if (!partitionAssignment.containsKey(pres.target)) {
            Log.info("Scale out");
            listener.scaling(newAssignment.size(), newAssignment);
        }
        //Scale in
        else if (partitionAssignment.get(pres.source).size() == pres.migratingPartitions.size()) {
            Log.info("Scale in");
            listener.scaling(newAssignment.size(), newAssignment);
        }
        //Load balance
        else {
            Log.info("Load balance");
            listener.changePartitionAssignment(newAssignment);
        }
    }
    Examiner examiner;
    //Main logic:  examine->diagnose->treatment->sleep
    @Override
    public void start(){
        int metricsRetreiveInterval = config.getInt("streamswitch.metrics.interval", 200);
        int metricsWarmupTime = config.getInt("streamswitch.metrics.warmup.time", 60000);
        waitForMigrationDeployed = false;
        startTime = System.currentTimeMillis();
        //Warm up phase
        LOG.info("Warm up for " + metricsWarmupTime + " milliseconds...");
        boolean isWarmup = true;
        do{
            long time = System.currentTimeMillis();
            if(time - startTime > metricsWarmupTime)isWarmup = false;
            else {
                try{
                    Thread.sleep(500l);
                }catch (Exception e){
                    LOG.error("Exception happens during warming up, ", e);
                }
            }
        }while(isWarmup);
        LOG.info("Warm up completed.");
        examiner = new Examiner(retriever);
        while(true) {
            //Examine
            Log.info("Examine...");
            long time = examiner.examine();
            Log.info("Diagnose...");
            Prescription pres = diagnose(time, examiner);
            if(pres.migratingPartitions != null){ //Not do nothing
                //Acquire lock for migration flag
                updateLock.lock();
                try {
                    doTreatment(pres);
                }finally {
                    updateLock.unlock();
                }
            }else{
                Log.info("Nothing to do this time.");
            }
            long deltaT = System.currentTimeMillis();
            if(deltaT - time > metricsRetreiveInterval){
                Log.warn("Run loop time is longer than interval, please consider to set larger interval");
            }
            Log.info("Sleep for " + deltaT + "milliseconds");
            try {
                Thread.sleep(metricsRetreiveInterval - (deltaT - time));
            }catch (Exception e) {
                Log.warn("Exception happens between run loop interval, ", e);
            }
        }
    }
    //TODO
    @Override
    public synchronized void onChangeImplemented(){
        LOG.info("Migration actually deployed, try to acquire lock...");
        updateLock.lock();
        try {
            LOG.info("Lock acquired, set migrating flag to false");
            if (examiner == null) {
                LOG.warn("Examiner haven't been initialized");
            } else if (!examiner.isMigrating) {
                LOG.warn("There is no pending migration, please checkout");
            } else {
                examiner.isMigrating = false;
                Map<String, List<String>> newAssignment = new HashMap<>(partitionAssignment);
                Prescription pres = examiner.pendingPres;
                LOG.info("Migrating " + pres.migratingPartitions + " from " + pres.source + " to " + pres.target);
                if (!newAssignment.containsKey(pres.target)) newAssignment.put(pres.target, new LinkedList<>());
                for (String partition : pres.migratingPartitions) {
                    newAssignment.get(pres.source).remove(partition);
                    newAssignment.get(pres.target).add(partition);
                }
                //For scale in
                if (newAssignment.get(pres.source).size() == 0) newAssignment.remove(pres.source);
                partitionAssignment = newAssignment;
                examiner.pendingPres = null;
            }
        }finally {
            updateLock.unlock();
            LOG.info("Migration complete, unlock");
        }
    }

    public void showData(){
        LOG.info("Show data:");
        state.showExecutors("");
        model.showData();

    }
}
