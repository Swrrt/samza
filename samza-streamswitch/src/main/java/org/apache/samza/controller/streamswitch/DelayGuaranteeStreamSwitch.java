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



    long migrationWarmupTime, migrationInterval, lastTime;
    double instantaneousThreshold, longTermThreshold;
    Examiner examiner;
    public DelayGuaranteeStreamSwitch(Config config){
        super(config);
        migrationWarmupTime = config.getLong("streamswitch.migration.warmup.time", 1000000000l);
        migrationInterval = config.getLong("streamswitch.migration.interval.time", 1000l);
        instantaneousThreshold = config.getDouble("streamswitch.delay.instant.threshold", 100.0);
        longTermThreshold = config.getDouble("streamswtich.delay.longterm.threshold", 100.0);
        lastTime = -1000000000l;
        algorithms = new Algorithms();
        updateLock = new ReentrantLock();
        examiner = new Examiner(retriever);
        examiner.model.setState(examiner.state);
        examiner.model.setTimes(config.getLong("streamswitch.delay.interval", 500l), config.getInt("streamswitch.delay.alpha", 20), config.getInt("streamswitch.delay.beta", 10));
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
        Map<String, List<String>> generateNewPartitionAssignment(Map<String, List<String>> oldAssignment){
            Map<String, List<String>> newAssignment = new HashMap<>(oldAssignment);
            if (!newAssignment.containsKey(target)) newAssignment.put(target, new LinkedList<>());
            for (String partition : migratingPartitions) {
                newAssignment.get(source).remove(partition);
                newAssignment.get(target).add(partition);
            }
            //For scale in
            if (newAssignment.get(source).size() == 0) newAssignment.remove(source);
            return newAssignment;
        }

    }


    //Algorithms packed
    protected class Algorithms {
        public Pair<Prescription, List<Pair<String, Double>>> tryToScaleOut(){
            LOG.info("Scale out by one container");

            if(partitionAssignment.size() <= 0){
                LOG.info("No executor to move");
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
            }
            long time = examiner.model.getCurrentTime();
            Pair<String, Double> a = findMaxLongtermDelayExecutor(partitionAssignment, time);
            String srcExecutor = a.getKey();
            double initialDelay = a.getValue();
            if(srcExecutor == null || srcExecutor.equals("") || partitionAssignment.get(srcExecutor).size() <=1){
                LOG.info("Cannot scale out: insufficient partition to migrate");
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
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
            return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(srcExecutor, tgtExecutor, migratingPartitions), null); //TODO: add here
        }
        public Pair<Prescription, List<Pair<String, Double>>> tryToScaleIn(){
            LOG.info("Try to scale in");
            long time = examiner.model.getCurrentTime();
            if(partitionAssignment.size() <= 1){
                LOG.info("Not enough executor to merge");
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
            }
            String minsrc = "", mintgt = "";
            double minLongtermDelay = -1;
            for(String src: partitionAssignment.keySet()){
                double srcArrival = examiner.model.getExecutorArrivalRate(src);
                for(String tgt: partitionAssignment.keySet())
                    if(!src.equals(tgt)){
                        double tgtArrival = examiner.model.getExecutorArrivalRate(tgt);
                        double tgtService = examiner.model.getExecutorServiceRate(tgt);
                        double tgtInstantDelay = examiner.model.getAvgDelay(tgt);
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
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(minsrc, mintgt, migratingPartitions), null); //TODO: add here
            }
            LOG.info("Cannot find any scale in");
            return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
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
                double partitionArrivalRate = examiner.model.getPartitionArriveRate(partition);
                return (partitionArrivalRate + tgtArrivalRate < tgtServiceRate - 1e-12);

            }

            protected void migratingPartition(String partition) {
                migratingPartitions.add(partition);
                double arrivalRate = examiner.model.getPartitionArriveRate(partition);
                srcArrivalRate -= arrivalRate;
                tgtArrivalRate += arrivalRate;
            }

            protected void unmigratingPartition(String partition) {
                migratingPartitions.remove(partition);
                double arrivalRate = examiner.model.getPartitionArriveRate(partition);
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
                double n2 = examiner.model.getExecutorArrivalRate(container);
                double u1 = dfsState.srcServiceRate;
                double u2 = examiner.model.getExecutorServiceRate(container);
                double instantDelay = examiner.model.getAvgDelay(container);
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
        //Return false if both instantaneous and long-term thresholds are violated
        public boolean checkDelay(String containerId){
            double delay = examiner.model.getAvgDelay(containerId);
            double longTermDelay = examiner.model.getLongTermDelay(containerId);
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

        private Pair<String, Double> findMaxLongtermDelayExecutor(Map<String, List<String>> partitionAssignment, long time){
            double initialDelay = -1.0;
            String maxExecutor = "";
            for (String executor : partitionAssignment.keySet()) {
                double longtermDelay = examiner.model.getLongTermDelay(executor);
                if (longtermDelay > initialDelay && !checkDelay(executor)) {
                    initialDelay = longtermDelay;
                    maxExecutor = executor;
                }
            }
            return new Pair(maxExecutor, initialDelay);
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
                double delay = examiner.model.getLongTermDelay(containerId);
                if (delay > initialDelay && !checkDelay(containerId)) {
                    initialDelay = delay;
                    maxContainer = containerId;
                }
            }
            return new Pair(maxContainer, initialDelay);
        }

        //TODO: implement DFS
        public Pair<Prescription, List<Pair<String, Double>>> tryToMigrate(){
            LOG.info("Try to migrate");
            LOG.info("Migrating once based on assignment: " + partitionAssignment);
            Map<String, List<String>> containerTasks = new HashMap<>();
            long time = examiner.model.getCurrentTime();
            containerTasks = partitionAssignment;
            if (containerTasks.keySet().size() == 0) { //No executor to move
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
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
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                return result;
            }

            if (containerTasks.get(srcContainer).size() <= 1) { //Container has only one partition
                LOG.info("Largest delay container " + srcContainer + " has only " + containerTasks.get(srcContainer).size());
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                return result;
            }
            LOG.info("Try to migrate from largest delay container " + srcContainer);
            dfsState.bestDelay = initialDelay;
            dfsState.bestSrcContainer = srcContainer;
            dfsState.bestTgtContainer = srcContainer;
            dfsState.bestMigration.clear();
            //Migrating this container
            dfsState.srcContainer = srcContainer;
            dfsState.srcArrivalRate = examiner.model.getExecutorArrivalRate(srcContainer);
            dfsState.srcServiceRate = examiner.model.getExecutorServiceRate(srcContainer);
            dfsState.srcPartitions = containerTasks.get(srcContainer);
            //Choose target container based on ideal delay (minimize ideal delay)
            a = findIdealLongtermContainer(dfsState, srcContainer, containerTasks, time);
            String tgtContainer = a.getKey();
            double minIdealDelay = a.getValue();
            if(tgtContainer.equals("")){
                LOG.info("Cannot find available migration");
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                return result;
            }
            LOG.info("Find minimal ideal container " + tgtContainer + " , ideal delay: " + minIdealDelay);
/*        for (String tgtContainer : containerTasks.keySet())
            if (!srcContainer.equals(tgtContainer)) {*/
            double tgtArrivalRate = examiner.model.getExecutorArrivalRate(tgtContainer);
            double tgtServiceRate = examiner.model.getExecutorServiceRate(tgtContainer);
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
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                return result;
            }

            if(dfsState.bestDelay > longTermThreshold){
                LOG.info("Cannot find migration smaller than threshold");
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
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
            Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(src, tgt, migratingTasks),longtermDelay);
            return result;
        }
    }

    class Examiner{
        class State {
            Map<String, Map<Long, Long>> partitionArrived, partitionCompleted; //Instead of actual time, use the n-th time point as key
            Map<String, Double> executorUtilization;
            Map<Long, Long> timePoints; //Actual time of n-th time point.
            long windowSize;
            long currentTimeIndex;
            public State() {
                timePoints = new HashMap<>();
                windowSize = 100000;
                currentTimeIndex = -1;
            }
            public void setWindowSize(long size){
                windowSize = size;
            }
            protected Map<Long, Long> getTimePoints(){
                return timePoints;
            }
            protected long getTimepoint(long n){
                return timePoints.get(n);
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
            public void updatePartitionArrived(String partitionId, long n, long arrived){
                partitionArrived.putIfAbsent(partitionId, new TreeMap<>());
                partitionArrived.get(partitionId).put(n, arrived);
            }
            public void updatePartitionCompleted(String partitionId, long n, long completed){
                partitionCompleted.putIfAbsent(partitionId, new TreeMap<>());
                partitionCompleted.get(partitionId).put(n, completed);
            }
            public void updateUtilization(String executorId, double utilization){
                executorUtilization.put(executorId, utilization);
            }
            public long getPartitionArrived(String partitionId, long n){
                long arrived = 0;
                if(partitionArrived.containsKey(partitionId)){
                    arrived = partitionArrived.get(partitionId).getOrDefault(n, 0l);
                }
                return arrived;
            }
            public long getPartitionCompleted(String partitionId, long n){
                long arrived = 0;
                if(partitionArrived.containsKey(partitionId)){
                    arrived = partitionArrived.get(partitionId).getOrDefault(n, 0l);
                }
                return arrived;
            }
            //Return -1 for no utilization
            public double getUtilization(String executorId){
                return executorUtilization.getOrDefault(executorId, -1.0);
            }
            //Remove data older than current index - window size
            private void popOldState(){
                for(String partition: partitionArrived.keySet()){
                    partitionArrived.get(partition).remove(currentTimeIndex - windowSize);
                    partitionCompleted.get(partition).remove(currentTimeIndex - windowSize);
                }
            }
            public long calculateArrivalTime(String partition, long r){
                for(Map.Entry<Long, Long> entry: partitionArrived.get(partition).entrySet()){
                    if(r <= entry.getValue())return entry.getKey();
                }
                return 0;
            }
            /*
                Replace invalid data in state with valid estimation
                when current snapshot is all valid
             */
            public void calibrate(long time){
                //Calibrate partition state
                for(String executor: partitionAssignment.keySet()){
                    for(String partitionId: partitionAssignment.get(executor)) {

                    }
                }
                //Calibrate executorId
            }

            public void updateAtTime(long time, Map<String, Long> taskArrived, Map<String, Long> taskProcessed, Map<String, Double> utilization, Map<String, List<String>> partitionAssignment) { //Normal update
                LOG.info("Debugging, time: " + time + " taskArrived: "+ taskArrived + " taskProcessed: "+ taskProcessed + " assignment: " + partitionAssignment);
                currentTimeIndex++;
                if(currentTimeIndex == 0){ //Initialize
                    for(String executor: partitionAssignment.keySet()){
                        for(String partition: partitionAssignment.get(executor)){
                            updatePartitionCompleted(partition, 0, 0);
                            updatePartitionArrived(partition, 0, 0);
                        }
                    }
                    timePoints.put(0l, 0l);
                    currentTimeIndex++;
                }
                timePoints.put(currentTimeIndex, time);
                for(String partition: taskArrived.keySet()){
                    updatePartitionArrived(partition, currentTimeIndex, taskArrived.get(partition));
                }
                for(String partition: taskProcessed.keySet()){
                    updatePartitionCompleted(partition, currentTimeIndex, taskProcessed.get(partition));
                }
                for(String executor: utilization.keySet()){
                    updateUtilization(executor, utilization.get(executor));
                }

            }
            private void writeLog(String string){
                System.out.println("DelayEstimator: " + string);
            }
        }
        class Model {
            private long lastTime;
            private State state;
            Map<String, Double> partitionArrivalRate, executorArrivalRate, serviceRate, instantaneousDelay;
            private Map<String, Deque<Pair<Long, Double>>> delayWindow; //Delay window stores <processed, delay> pair
            private Map<String, Deque<Pair<Long, Double>>> utilizationWindow; //Utilization stores <time, utilization> pair
            private Map<String, Deque<Pair<Long, Double>>> serviceWindow; //Utilization stores <time, processed> pair
            private int alpha = 1, beta = 2;
            private long interval = 0;
            public Model(){
                lastTime = -1;
                delayWindow = new HashMap<>();
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

            private double calculatePartitionInstantDelay(String partition, long n){
                long cn = state.getPartitionCompleted(partition, n), cn_1 = state.getPartitionCompleted(partition, n - 1);
                long m0 = state.calculateArrivalTime(partition, cn_1 + 1), m1 = state.calculateArrivalTime(partition, cn);
                long am0 = state.getPartitionArrived(partition, m0), am1 = state.getPartitionArrived(partition, m1);
                long M = (am0 - cn_1) * m0 - (am1 - cn) * m1;
                for(long m = state.calculateArrivalTime(partition, cn_1) + 1; m <= m1; m++){
                    long am = state.getPartitionArrived(partition, m), am_1 = state.getPartitionArrived(partition, m - 1);
                    M += (am - am_1) * m;
                }
                long T = state.getTimepoint(n) - state.getTimepoint(n - 1);
                return (n + 1 - M / ((double)(cn - cn_1))) * T;
            }

            private double calculateExecutorInstantDelay(String executor, long n){
                double totalDelay = 0;
                long totalCompleted = 0;
                for(String partition: partitionAssignment.get(executor)){
                    long completed = state.getPartitionCompleted(partition, n) - state.getPartitionCompleted(partition, n-1);
                    totalDelay += calculatePartitionInstantDelay(partition, n) * completed;
                    totalCompleted += completed;
                }
                double delay = 0;
                if(totalCompleted > 0)delay = totalDelay / totalCompleted;
                return delay;
            }


            // 1 / ( u - n ). Return  1e100 if u <= n
            private double getLongTermDelay(String executorId){
                double arrival = executorArrivalRate.get(executorId);
                double service = serviceRate.get(executorId);
                if(service < arrival + 1e-15)return 1e100;
                return 1.0/(service - arrival);
            }

            private double getPartitionArrivalRate(String partition, long n0, long n1){
                long totalArrived = 0;
                double time = state.getTimepoint(n1) - state.getTimepoint(n0);
                totalArrived = state.getPartitionArrived(partition, n1) - state.getPartitionArrived(partition, n0);
                double arrivalRate = 0;
                if(time > 1e-9)arrivalRate = totalArrived / time;
                return arrivalRate;
            }
            // Calculate window arrival rate of tn0 ~ tn1 (exclude tn0)
            private double getExecutorArrivalRate(String executorId, long n0, long n1, Map<String, List<String>> partitionAssignment){
                double arrivalRate = 0;
                for(String partition: partitionAssignment.get(executorId)){
                    arrivalRate += getPartitionArrivalRate(partition, n0, n1);
                }
                return arrivalRate;
            }

            private void updateWindowExecutorServiced(String executor, long n, Map<String, List<String>> partitionAssignment){
                long processed = 0;
                for(String partition: partitionAssignment.get(executor)){
                    processed = state.getPartitionCompleted(partition, n) - state.getPartitionCompleted(partition, n - 1);
                }

                if(!serviceWindow.containsKey(executor)){
                    serviceWindow.put(executor, new LinkedList<>());
                }
                serviceWindow.get(executor).addLast(new Pair(n, processed));
                if(serviceWindow.size() > beta){
                    serviceWindow.get(executor).pollFirst();
                }
            }

            // Calculate window service rate of tn0 ~ tn1 (exclude tn0)
            private double getExecutorServiceRate(String executorId){
                double totalService = 0;
                long totalTime = 0;
                for(Pair<Long, Double> entry: serviceWindow.get(executorId)){
                    long time = state.getTimepoint(entry.getKey()) - state.getTimepoint(entry.getKey() - 1);
                    totalTime += time;
                    totalService += entry.getValue() * time;
                }
                if(totalTime > 0) totalService /= totalTime;
                return totalService;
            }

            private void updateWindowExecutorUtilization(String executor, long n){
                double util = state.getUtilization(executor);
                if(!utilizationWindow.containsKey(executor)){
                    utilizationWindow.put(executor, new LinkedList<>());
                }
                utilizationWindow.get(executor).addLast(new Pair(n, util));
                if(utilizationWindow.size() > beta){
                    utilizationWindow.get(executor).pollFirst();
                }
            }

            // Window average utilization
            private double getWindowExecutorUtilization(String executorId){
                double totalUtilization = 0;
                long totalTime = 0;
                for(Pair<Long, Double> entry: utilizationWindow.get(executorId)){
                    long time = state.getTimepoint(entry.getKey()) - state.getTimepoint(entry.getKey() - 1);
                    totalTime += time;
                    totalUtilization += entry.getValue() * time;
                }
                if(totalTime > 0) totalUtilization/= totalTime;
                return totalUtilization;
            }
            private void updateWindowExecutorInstantaneousDelay(String executor, long n, Map<String, List<String>> partitionAssignment){
                double instantDelay = calculateExecutorInstantDelay(executor, n);
                if(!delayWindow.containsKey(executor)){
                    delayWindow.put(executor, new LinkedList<>());
                }
                long processed = 0;
                for(String partition: partitionAssignment.get(executor)){
                    processed += state.getPartitionCompleted(partition, n) - state.getPartitionCompleted(partition, n - 1);
                }
                delayWindow.get(executor).addLast(new Pair(processed, instantDelay));
                if(delayWindow.size() > beta){
                    delayWindow.get(executor).pollFirst();
                }
            }
            //Window average delay
            public double getWindowExecutorInstantaneousDelay(String executorId){
                double totalDelay = 0;
                long totalProcessed = 0;
                for(Pair<Long, Double> entry: delayWindow.get(executorId)){
                    long processed = entry.getKey();
                    totalProcessed += processed;
                    totalDelay += entry.getValue() * processed;
                }
                if(totalProcessed > 0) totalDelay/=totalProcessed;
                return totalDelay;
            }

            //Update snapshots from state
            public void updateModelSnapshot(long n, Map<String, List<String>> partitionAssignment){

                partitionArrivalRate.clear();
                executorArrivalRate.clear();
                serviceRate.clear();
                instantaneousDelay.clear();

                for(String executor: partitionAssignment.keySet()){
                    double arrivalRate = 0;
                    for(String partition: partitionAssignment.get(executor)){
                        double t = getPartitionArrivalRate(partition, n, n - beta);
                        partitionArrivalRate.put(partition, t);
                        arrivalRate += t;
                    }
                    executorArrivalRate.put(executor, arrivalRate);
                    updateWindowExecutorUtilization(executor, n);
                    double util = getWindowExecutorUtilization(executor);
                    updateWindowExecutorServiced(executor, n, partitionAssignment);
                    double mu = getExecutorServiceRate(executor);
                    if(util > 1e-9 && util <= 1){
                        mu /= util;
                    }
                    serviceRate.put(executor, mu);
                    updateWindowExecutorInstantaneousDelay(executor, n, partitionAssignment);
                    instantaneousDelay.put(executor, getWindowExecutorInstantaneousDelay(executor));
                }
            }
            public void showData(){
                LOG.info("Show delay estimation data...");
                LOG.info("Partition arrival rate:");
            }
        }
        private Model model;
        private State state;
        private StreamSwitchMetricsRetriever metricsRetriever;
        private boolean isValid, isMigrating;
        private Prescription pendingPres;
        Examiner(StreamSwitchMetricsRetriever metricsRetriever){
            this.metricsRetriever = metricsRetriever;
            this.state = new State();
            this.model = new Model();

            isValid = false;//No data, should be false
        }
        private void examine(long time){
            Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
            Map<String, Long> partitionArrived =
                    (HashMap<String, Long>) (metrics.get("PartitionArrived"));
            Map<String, Long> partitionProcessed =
                    (HashMap<String, Long>) (metrics.get("PartitionProcessed"));
            Map<String, Double> executorUtilization =
                    (HashMap<String, Double>) (metrics.get("ExecutorUtilization"));
            //TODO: check valid or not here
            updateState(time, partitionArrived, partitionProcessed, executorUtilization);
            updateModel(time);
            isValid = true;
            for(String executor: partitionAssignment.keySet()){
                if(!model.executorArrivalRate.containsKey(executor)){
                    LOG.info("Current model is not valid, because " + executor + " is not ready.");
                    isValid = false;
                    break;
                }
                if(!executorUtilization.containsKey(executor)){
                    LOG.info("Current state is not valid, because " + executor + " utilization is missing");
                    isValid = false;
                    break;
                }
                for(String partition: partitionAssignment.get(executor)){
                    if(!partitionArrived.containsKey(partition)){
                        LOG.info("Current state is not valid, because " + partition + " arrived is missing");
                        isValid = false;
                        break;
                    }
                    if(!partitionProcessed.containsKey(partition)){
                        LOG.info("Current state is not valid, because " + executor + " processed is missing");
                        isValid = false;
                        break;
                    }
                }
                if(!isValid)break;
            }
        }
        private void updateState(long time, Map<String, Long> partitionArrived, Map<String, Long> partitionProcessed, Map<String, Double> executorUtilization){
            LOG.info("Updating network calculus model...");
            state.updateAtTime(time, partitionArrived, partitionProcessed, executorUtilization, partitionAssignment);
        }

        private void updateModel(long time){
            LOG.info("Updating Delay Estimating model");
            model.updateModelSnapshot(time, partitionAssignment);

            //Debug & Statistics
            if(true){
                HashMap<String, Double> longtermDelay = new HashMap<>();
                for(String executorId: partitionAssignment.keySet()){
                    double delay = model.getLongTermDelay(executorId);
                    longtermDelay.put(executorId, delay);
                }
                System.out.println("Model, time " + time + " : " + "Arrival Rate: " + model.executorArrivalRate);
                System.out.println("Model, time " + time + " : " + "Service Rate: " + model.serviceRate);
                System.out.println("Model, time " + time + " : " + "Average Delay: " + model.instantaneousDelay);
                System.out.println("Model, time " + time + " : " + "Longterm Delay: " + longtermDelay);
                System.out.println("Model, time " + time + " : " + "Partition Arrival Rate: " + model.partitionArrivalRate);
            }
        }
        private List<Pair<String, Double>> getInstantDelay(){
            List<Pair<String, Double>> delay = new LinkedList<>();
            for(String executor: partitionAssignment.keySet()){
                delay.add(new Pair(executor, model.instantaneousDelay.get(executor)));
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

        private Pair<Prescription, List<Pair<String, Double>>> loadBalance(){
            return algorithms.tryToMigrate();
        }
        private Pair<Prescription, List<Pair<String, Double>>> scaleIn(){
            return algorithms.tryToScaleIn();
        }
        private Pair<Prescription, List<Pair<String, Double>>> scaleOut(){
            return algorithms.tryToScaleOut();
        }
    }

    Algorithms algorithms;


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
    private Prescription diagnose(Examiner examiner){
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
            Pair<Prescription, List<Pair<String, Double>>> result = examiner.scaleIn();
            if(result.getValue() != null) {
                int thealthiness = checkHealthiness(examiner.getInstantDelay(), result.getValue());
                if (thealthiness == 0) {  //Scale in OK
                    return result.getKey();
                }
            }
            //Do nothing
            return pres;
        }
        //Severe
        else{
            Pair<Prescription, List<Pair<String, Double>>> result = examiner.loadBalance();
            if(result.getValue() != null) {
                int thealthiness = checkHealthiness(examiner.getInstantDelay(), result.getValue());
                if (thealthiness == 1) {  //Load balance OK
                    return result.getKey();
                }
            }
            //Scale out
            result = examiner.scaleOut();
            return result.getKey();
        }
    }
    //Treatment for Samza
    private void treat(Prescription pres){
        if(pres.migratingPartitions == null){
            Log.warn("Prescription has nothing, so do no treatment");
            return ;
        }

        examiner.pendingPres = pres;
        examiner.isMigrating = true;

        Map<String, List<String>> newAssignment = pres.generateNewPartitionAssignment(partitionAssignment);
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
    //Main logic:  examine->diagnose->treatment->sleep
    @Override
    public void start(){
        int metricsRetreiveInterval = config.getInt("streamswitch.metrics.interval", 200);
        int metricsWarmupTime = config.getInt("streamswitch.metrics.warmup.time", 60000);
        startTime = System.currentTimeMillis();
        //Warm up phase
        LOG.info("Warm up for " + metricsWarmupTime + " milliseconds...");
        do{
            long time = System.currentTimeMillis();
            if(time - startTime > metricsWarmupTime){
                break;
            }
            else {
                try{
                    Thread.sleep(500l);
                }catch (Exception e){
                    LOG.error("Exception happens during warming up, ", e);
                }
            }
        }while(true);
        LOG.info("Warm up completed.");
        while(true) {
            //Examine
            Log.info("Examine...");
            long time = System.currentTimeMillis();
            examiner.examine(time);
            Log.info("Diagnose...");
            if(examiner.isValid && !examiner.isMigrating) {
                Prescription pres = diagnose(examiner);
                if (pres.migratingPartitions != null) { //Not do nothing
                    treat(pres);
                } else {
                    Log.info("Nothing to do this time.");
                }
            }else{
                if(!examiner.isValid)LOG.info("Current examine data is not valid, need to wait until valid");
                else LOG.info("One migration is in process");
            }
            long deltaT = System.currentTimeMillis() - time;
            if(deltaT > metricsRetreiveInterval){
                Log.warn("Run loop time is longer than interval, please consider to set larger interval");
            }
            Log.info("Sleep for " + (metricsRetreiveInterval - deltaT) + "milliseconds");
            try {
                Thread.sleep(metricsRetreiveInterval - deltaT);
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
                Prescription pres = examiner.pendingPres;
                LOG.info("Migrating " + pres.migratingPartitions + " from " + pres.source + " to " + pres.target);
                partitionAssignment = pres.generateNewPartitionAssignment(partitionAssignment);
                examiner.pendingPres = null;
            }
        }finally {
            updateLock.unlock();
            LOG.info("Migration complete, unlock");
        }
    }
}
