package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.samza.config.Config;
import org.apache.samza.controller.OperatorControllerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//Under development

public class LatencyGuarantor extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyGuarantor.class);
    private long latencyReq, windowReq; //Window requirment is stored as number of timeslot
    private double alpha, beta; // Check with latencyReq * coefficient. instantDelay * alpha < req and longtermDelay * beta < req
    private Prescription pendingPres;
    private Examiner examiner;
    public LatencyGuarantor(Config config){
        super(config);
        latencyReq = config.getLong("streamswitch.requirement.latency", 400); //Unit: millisecond
        windowReq = config.getLong("streamswitch.requirement.window", 1000) / metricsRetreiveInterval; //Unit: # of time slots
        alpha = config.getDouble("streamswitch.system.alpha", 0.8);
        beta = config.getDouble("streamswitch.system.beta", 0.8);
        examiner = new Examiner();
        pendingPres = null;
    }

    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions) {
        super.init(listener, executors, partitions);
        //TODO: Initialize state/model here
        examiner.init(executorMapping);
    }

    class Examiner{
        class State {
            Map<String, Map<Long, Long>> partitionArrived, partitionCompleted; //Instead of actual time, use the n-th time point as key
            Map<String, Long> partitionLastValid; //Last valid state time point
            private Map<Long, Map<String, List<String>>> mappings;
            private Map<String, Map<Long, Double>> executorUtilizations;
            //For calculate instant delay
            private Map<String, Map<Long, Long>> partitionTotalLatency;
            private Map<String, Long> partitionArrivedIndex;
            private Map<String, Long> partitionArrivedRemainedIndex;
            private long lastValidTimeIndex;
            long currentTimeIndex;
            long storedTimeWindowSize;
            private State() {
                currentTimeIndex = 0;
                lastValidTimeIndex = 0;
                storedTimeWindowSize = Math.max(100, windowReq + 2);
                partitionArrived = new HashMap<>();
                partitionCompleted = new HashMap<>();
                executorUtilizations = new HashMap<>();
                partitionLastValid = new HashMap<>();
                mappings = new HashMap<>();
                partitionTotalLatency = new HashMap<>();
                partitionArrivedIndex = new HashMap<>();
                partitionArrivedRemainedIndex = new HashMap<>();
            }

            private void init(Map<String, List<String>> executorMapping){
                LOG.info("Initialize time point 0...");
                for (String executor : executorMapping.keySet()) {
                    for (String partition : executorMapping.get(executor)) {
                        partitionArrived.put(partition, new HashMap<>());
                        partitionArrived.get(partition).put(0l, 0l);
                        partitionCompleted.put(partition, new HashMap<>());
                        partitionCompleted.get(partition).put(0l, 0l);
                        partitionLastValid.put(partition, 0l);
                    }
                }
            }

            protected long getTimepoint(long timeIndex){
                return timeIndex * metricsRetreiveInterval;
            }

            protected Map<String, List<String>> getMapping(long n){
                if(mappings.containsKey(n))return mappings.get(n);
                else return null;
            }
            public long getPartitionArrived(String partitionId, long n){
                long arrived = 0;
                if(partitionArrived.containsKey(partitionId)){
                    arrived = partitionArrived.get(partitionId).getOrDefault(n, 0l);
                }
                return arrived;
            }
            public long getPartitionCompleted(String partitionId, long n){
                long completed = 0;
                if(partitionCompleted.containsKey(partitionId)){
                    completed = partitionCompleted.get(partitionId).getOrDefault(n, 0l);
                }
                return completed;
            }

            private double getAverageExecutorUtilization(String executorId){
                double totalUtilization = 0;
                long totalTime = 0;
                for(Map.Entry<Long, Double> entry: executorUtilizations.get(executorId).entrySet()){
                    long time = state.getTimepoint(entry.getKey()) - state.getTimepoint(entry.getKey() - 1);
                    totalTime += time;
                    totalUtilization += entry.getValue() * time;
                }
                if(totalTime > 0) totalUtilization/= totalTime;
                return totalUtilization;
            }


            public long getPartitionLatency(String partition, long timeIndex){
                long latency = 0;
                if(partitionTotalLatency.containsKey(partition)){
                    latency = partitionTotalLatency.get(partition).getOrDefault(timeIndex, 0l);
                }
                return latency;
            }

            //Calculate the total latency in new slot. Drop useless data during calculation.
            private void calculatePartition(String partition, long timeIndex){
                //Calculate total latency and # of tuples in current time slots
                long arrivalIndex = partitionArrivedIndex.getOrDefault(partition, 1l);
                long complete = getPartitionCompleted(partition, timeIndex);
                long lastComplete = getPartitionCompleted(partition, timeIndex - 1);
                long arrived = getPartitionArrived(partition, arrivalIndex);
                long lastArrived = getPartitionArrived(partition, arrivalIndex - 1);
                long totalDelay = 0;
                while(arrivalIndex <= timeIndex && lastArrived < complete){
                    if(arrived > lastComplete){ //Should count into this slot
                        long number = Math.min(complete, arrived) - Math.max(lastComplete, lastArrived);
                        totalDelay += number * (timeIndex - arrivalIndex + 1);
                    }
                    arrivalIndex++;
                }
                arrivalIndex--; //TODO: check here
                partitionTotalLatency.putIfAbsent(partition, new HashMap<>());
                partitionTotalLatency.get(partition).put(timeIndex, totalDelay);
                if(partitionTotalLatency.get(partition).containsKey(timeIndex - windowReq)){
                    partitionTotalLatency.get(partition).remove(timeIndex - windowReq);
                }
                partitionArrivedIndex.put(partition, arrivalIndex);
            }

            private void calculate(long timeIndex){
                for(long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
                    //Calculate latency from last valid time index
                    for (String executor : executorMapping.keySet()) {
                        for (String partition : executorMapping.get(executor)) {
                            calculatePartition(partition, index);
                        }
                    }
                    LOG.info("Debugging, time=" + index + " arrival index=" + partitionArrivedIndex);
                    LOG.info("Debugging, time=" + index + " total latency=" + partitionTotalLatency);
                }
            }

            private void drop(long timeIndex){
                //Drop arrived
                for (String executor : executorMapping.keySet()) {
                    for (String partition : executorMapping.get(executor)) {
                        long remainedIndex = partitionArrivedRemainedIndex.getOrDefault(partition, 0l);
                        while(remainedIndex < partitionArrivedIndex.get(partition) - 1 && remainedIndex < timeIndex - windowReq){
                            partitionArrived.get(partition).remove(remainedIndex);
                            remainedIndex++;
                        }
                        partitionArrivedRemainedIndex.put(partition, remainedIndex);
                    }
                }

                //Drop completed, utilization, mappings. These are fixed window
                for(long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
                    for (String executor : executorMapping.keySet()) {
                        for (String partition : executorMapping.get(executor)) {
                            //Drop completed
                            if (partitionCompleted.containsKey(partition) && partitionCompleted.get(partition).containsKey(index - windowReq - 1)) {
                                partitionCompleted.get(partition).remove(index - windowReq - 1);
                            }
                        }
                        //Drop utilization
                        if (executorUtilizations.containsKey(executor) && executorUtilizations.get(executor).containsKey(index - windowReq)) {
                            executorUtilizations.get(executor).remove(index - windowReq);
                        }
                    }
                    //Drop mappings
                    if (mappings.containsKey(index - windowReq)) mappings.remove(index - windowReq);
                }
                lastValidTimeIndex = timeIndex;
            }

            //Only called when time n is valid, also update partitionLastValid
            private void calibratePartition(String partition, long timeIndex){
                long n0 = partitionLastValid.getOrDefault(partition, 0l);
                partitionLastValid.put(partition, timeIndex);
                if(n0 < timeIndex - 1) {
                    //LOG.info("Calibrate state for " + partition + " from time=" + n0);
                    long a0 = state.getPartitionArrived(partition, n0);
                    long c0 = state.getPartitionCompleted(partition, n0);
                    long a1 = state.getPartitionArrived(partition, timeIndex);
                    long c1 = state.getPartitionCompleted(partition, timeIndex);
                    for (long i = n0 + 1; i < timeIndex; i++) {
                        long ai = (a1 - a0) * (i - n0) / (timeIndex - n0) + a0;
                        long ci = (c1 - c0) * (i - n0) / (timeIndex - n0) + c0;
                        state.partitionArrived.get(partition).put(i, ai);
                        state.partitionCompleted.get(partition).put(i, ci);
                    }
                }
            }

            //Calibrate whole state including mappings and utilizations
            private void calibrate(Map<String, Boolean> partitionValid){
                //Calibrate mappings
                if(mappings.containsKey(currentTimeIndex)){
                    for(long t = currentTimeIndex - 1; t >= 0 && t >= currentTimeIndex - windowReq - 1; t--){
                        if(!mappings.containsKey(t)){
                            mappings.put(t, mappings.get(currentTimeIndex));
                        }else{
                            break;
                        }
                    }
                }

                //Calibrate executor utilizations
                for(String executor: executorMapping.keySet()) {
                    if (executorUtilizations.containsKey(executor) && executorUtilizations.get(executor).containsKey(currentTimeIndex)) {
                        Map<Long, Double> utilization = executorUtilizations.get(executor);
                        long lastValid = -1;
                        for(long t: utilization.keySet())
                            if(t < currentTimeIndex  && t > lastValid) lastValid = t;
                        if(lastValid != -1) {
                            double ul = utilization.get(lastValid), ur = utilization.get(currentTimeIndex );
                            //Esitmate utilization
                            for (long t = lastValid + 1; t < currentTimeIndex ; t++) {
                                double util = ul + (t - lastValid) * (ur - ul) / (currentTimeIndex - lastValid);
                                utilization.put(t, util);
                            }
                        }else{
                            //We only have time n's utilization
                            for(long t = currentTimeIndex  - 1; t >= 0 && t >= currentTimeIndex  - windowReq - 1; t--){
                                utilization.put(t, utilization.get(currentTimeIndex ));
                            }
                        }
                    }
                }

                //Calibrate partitions' state
                if(partitionValid == null)return ;
                for(String partition: partitionValid.keySet()){
                    if (partitionValid.get(partition)){
                        calibratePartition(partition, currentTimeIndex);
                    }
                }
            }



            public void insert(long timeIndex, Map<String, Long> taskArrived,
                               Map<String, Long> taskProcessed, Map<String, Double> utilization,
                               Map<String, List<String>> executorMapping) { //Normal update
                LOG.info("Debugging, metrics retrieved data, time: " + timeIndex + " taskArrived: "+ taskArrived + " taskProcessed: "+ taskProcessed + " assignment: " + executorMapping);

                currentTimeIndex = timeIndex;
                mappings.put(currentTimeIndex, executorMapping);

                LOG.info("Current time " + timeIndex);

                for(String partition: taskArrived.keySet()){
                    partitionArrived.putIfAbsent(partition, new HashMap<>());
                    partitionArrived.get(partition).put(currentTimeIndex, taskArrived.get(partition));
                }
                for(String partition: taskProcessed.keySet()){
                    partitionCompleted.putIfAbsent(partition, new HashMap<>());
                    partitionCompleted.get(partition).put(currentTimeIndex, taskProcessed.get(partition));
                }
                for(String executor: utilization.keySet()){
                   executorUtilizations.putIfAbsent(executor, new HashMap<>());
                   executorUtilizations.get(executor).put(currentTimeIndex, utilization.get(executor));
                }
            }
        }
        //Model here is only a snapshot
        class Model {
            private State state;
            Map<String, Double> partitionArrivalRate, executorArrivalRate, executorServiceRate, executorInstantaneousDelay; //Longterm delay could be calculated from arrival rate and service rate
            public Model(State state){
                partitionArrivalRate = new HashMap<>();
                executorArrivalRate = new HashMap<>();
                executorServiceRate = new HashMap<>();
                executorInstantaneousDelay = new HashMap<>();
                this.state = state;
            }

            // 1 / ( u - n ). Return  1e100 if u <= n
            private double getLongTermDelay(String executorId){
                double arrival = executorArrivalRate.get(executorId);
                double service = executorServiceRate.get(executorId);
                if(arrival < 1e-15)return 0.0;
                if(service < arrival + 1e-15)return 1e100;
                return 1.0/(service - arrival);
            }

            private double calculatePartitionArrivalRate(String partition, long n0, long n1){
                if(n0 < 0)n0 = 0;
                long time = state.getTimepoint(n1) - state.getTimepoint(n0);
                long totalArrived = state.getPartitionArrived(partition, n1) - state.getPartitionArrived(partition, n0);
                double arrivalRate = 0;
                if(time > 1e-9)arrivalRate = totalArrived / ((double)time);
                return arrivalRate;
            }

            // Calculate window service rate of n - beta ~ n (exclude n - beta)
            private double calculateExecutorServiceRate(String executorId, long n){
                long totalCompleted = 0;
                long totalTime = 0;
                long n0 = n - windowReq + 1;
                if(n0 < 1)n0 = 1;
                for(long i = n0; i <= n; i++){
                    long time = state.getTimepoint(i) - state.getTimepoint(i - 1);
                    totalTime += time;
                    if(state.getMapping(i).containsKey(executorId)){ //Could be scaled out or scaled in
                        for(String partition: state.getMapping(i).get(executorId)) {
                            totalCompleted += state.getPartitionCompleted(partition, i) - state.getPartitionCompleted(partition, i-1);
                        }
                    }
                }
                if(totalTime > 0) return totalCompleted/((double)totalTime);
                return 0;
            }

            //Window average delay
            public double calculateExecutorInstantaneousDelay(String executorId, long timeIndex){
                long totalDelay = 0;
                long totalCompleted = 0;
                long n0 = timeIndex - windowReq + 1;
                if(n0<1){
                    n0 = 1;
                    LOG.warn("Calculate instant delay index smaller than window size!");
                }
                for(long i = n0; i <= timeIndex; i++){
                    if(state.getMapping(i).containsKey(executorId)){
                        for(String partition: state.getMapping(i).get(executorId)){
                            totalCompleted += state.getPartitionCompleted(partition, i) - state.getPartitionCompleted(partition, i - 1);
                            totalDelay += state.getPartitionLatency(partition, i);
                        }
                    }
                }
                //In state, latency is count as # of timeslots, need to transfer to real time
                if(totalCompleted > 0) return totalDelay * metricsRetreiveInterval / ((double)totalCompleted);
                return 0;
            }

            //Calculate model snapshot from state
            public void update(long timeIndex, Map<String, List<String>> executorMapping){
                LOG.info("Updating model snapshot, clear old data...");
                partitionArrivalRate.clear();
                executorArrivalRate.clear();
                executorServiceRate.clear();
                executorInstantaneousDelay.clear();
                for(String executor: executorMapping.keySet()){
                    double arrivalRate = 0;
                    for(String partition: executorMapping.get(executor)){
                        double t = calculatePartitionArrivalRate(partition, timeIndex - windowReq, timeIndex);
                        partitionArrivalRate.put(partition, t);
                        arrivalRate += t;
                    }
                    executorArrivalRate.put(executor, arrivalRate);
                    double util = state.getAverageExecutorUtilization(executor);
                    double mu = calculateExecutorServiceRate(executor, timeIndex);
                    if(util > 1e-9 && util <= 1){
                        mu /= util;
                    }
                    executorServiceRate.put(executor, mu);
                    executorInstantaneousDelay.put(executor, calculateExecutorInstantaneousDelay(executor, timeIndex));
                }
                LOG.info("Debugging, partition arrival rate: " + partitionArrivalRate);
                LOG.info("Debugging, executor windowed service: " + executorServiceRate);
                LOG.info("Debugging, executor windowed delay: " + executorInstantaneousDelay);
            }
        }

        private Model model;
        private State state;
        Examiner(){
            state = new State();
            model = new Model(state);
            lastMigratedTime = 0l;
        }

        private void init(Map<String, List<String>> executorMapping){
            state.init(executorMapping);
        }

        private boolean checkValidity(Map<String, Boolean> partitionValid){
            if(partitionValid == null)return false;

            //Current we don't haven enough time slot to calculate model
            if(state.currentTimeIndex < windowReq){
                LOG.info("Current time slots number is smaller than beta, not valid");
                return false;
            }
            //Partition Metrics Valid
            for(String executor: executorMapping.keySet()) {
                for (String partition : executorMapping.get(executor)) {
                    if (!partitionValid.containsKey(partition) || !partitionValid.get(partition)) {
                        LOG.info(partition + "'s metrics is not valid");
                        return false;
                    }
                }
            }

            //State Valid
            for(String executor: executorMapping.keySet()){
                if(!state.executorUtilizations.containsKey(executor)){
                    LOG.info("Current state is not valid, because " + executor + " utilization is missing");
                    return false;
                }
                for(String partition: executorMapping.get(executor)){
                    if(!state.partitionArrived.containsKey(partition)){
                        LOG.info("Current state is not valid, because " + partition + " arrived is missing");
                        return false;
                    }
                    if(!state.partitionCompleted.containsKey(partition)){
                        LOG.info("Current state is not valid, because " + executor + " processed is missing");
                        return false;
                    }
                }
            }
            return true;
        }


        private boolean updateState(long timeIndex, Map<String, Long> partitionArrived, Map<String, Long> partitionProcessed, Map<String, Double> executorUtilization, Map<String, Boolean> partitionValid, Map<String, List<String>> executorMapping){
            LOG.info("Updating state...");
            state.insert(timeIndex, partitionArrived, partitionProcessed, executorUtilization, executorMapping);
            state.calibrate(partitionValid);

            //Debug & Statistics
            HashMap<String, Long> arrived = new HashMap<>(), completed = new HashMap<>();
            for(String partition: state.partitionArrived.keySet()) {
                arrived.put(partition, state.partitionArrived.get(partition).get(state.currentTimeIndex));
                completed.put(partition, state.partitionCompleted.get(partition).get(state.currentTimeIndex));
            }
            System.out.println("State, time " + timeIndex  + " , Partition Arrived: " + arrived);
            System.out.println("State, time " + timeIndex  + " , Partition Completed: " + completed);

            if(checkValidity(partitionValid)){
                state.calculate(timeIndex);
                state.drop(timeIndex);
                return true;
            }else return false;
        }

        private void updateModel(long timeIndex, Map<String, List<String>> executorMapping){
            LOG.info("Updating Model");
            model.update(timeIndex, executorMapping);

            //Debug & Statistics
            if(true){
                HashMap<String, Double> longtermDelay = new HashMap<>();
                for(String executorId: executorMapping.keySet()){
                    double delay = model.getLongTermDelay(executorId);
                    longtermDelay.put(executorId, delay);
                }
                long time = timeIndex;
                System.out.println("Model, time " + time  + " , Arrival Rate: " + model.executorArrivalRate);
                System.out.println("Model, time " + time  + " , Service Rate: " + model.executorServiceRate);
                System.out.println("Model, time " + time  + " , Instantaneous Delay: " + model.executorInstantaneousDelay);
                System.out.println("Model, time " + time  + " , Longterm Delay: " + longtermDelay);
                System.out.println("Model, time " + time  + " , Partition Arrival Rate: " + model.partitionArrivalRate);
            }
        }
        private Map<String, Double> getInstantDelay(){
            return model.executorInstantaneousDelay;
        }

        private Map<String, Double> getLongtermDelay(){
            HashMap<String, Double> delay = new HashMap<>();
            for(String executor: executorMapping.keySet()){
                delay.put(executor, model.getLongTermDelay(executor));
            }
            return delay;
        }
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
            Map<String, List<String>> newAssignment = new HashMap<>();
            for(String executor: oldAssignment.keySet()){
                List<String> partitions = new ArrayList<>(oldAssignment.get(executor));
                newAssignment.put(executor, partitions);
            }
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
    private Prescription diagnose(Examiner examiner){

        class Diagnoser { //Algorithms and utility methods
            // Find the subset which minimizes delay by greedy:
            // Sort arrival rate from largest to smallest, cut them in somewhere
            private Pair<Prescription, Map<String, Double>> tryToScaleOut(){
                LOG.info("Scale out by one container");
                if(executorMapping.size() <= 0){
                    LOG.info("No executor to move");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
                Pair<String, Double> a = findMaxLongtermDelayExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if(srcExecutor == null || srcExecutor.equals("") || executorMapping.get(srcExecutor).size() <=1){
                    LOG.info("Cannot scale out: insufficient partition to migrate");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                LOG.info("Migrating out from executor " + srcExecutor);

                //Try from smallest latency to largest latency
                TreeMap<Double, Set<String>> partitions = new TreeMap<>();

                for(String partition: executorMapping.get(srcExecutor)){
                    double delay = examiner.state.getPartitionLatency(partition, examiner.state.currentTimeIndex) /
                            ((double) examiner.state.getPartitionCompleted(partition, examiner.state.currentTimeIndex) - examiner.state.getPartitionCompleted(partition, examiner.state.currentTimeIndex - 1));
                    partitions.putIfAbsent(delay, new HashSet());
                    partitions.get(delay).add(partition);
                }
                //Debugging
                LOG.info("Debugging, partitions' latency=" + partitions);

                List<String> sortedPartitions = new LinkedList<>();
                for(Map.Entry<Double, Set<String>> entry: partitions.entrySet()){
                    sortedPartitions.addAll(entry.getValue());
                }

                double arrivalrate0 = examiner.model.executorArrivalRate.get(srcExecutor), arrivalrate1 = 0;
                //double executorServiceRate = examiner.model.executorServiceRate.get(srcExecutor); //Assume new executor has same service rate
                double best = 1e100;
                List<String> migratingPartitions = new ArrayList<>();
                for(String partition: sortedPartitions){
                    double arrival = examiner.model.partitionArrivalRate.get(partition);
                    arrivalrate0 -= arrival;
                    arrivalrate1 += arrival;

                    if(Math.max(arrivalrate0, arrivalrate1) < best){
                        best = Math.max(arrivalrate0, arrivalrate1);
                        migratingPartitions.add(partition);
                    }
                    if(arrivalrate0 < arrivalrate1)break;
                }
                long newExecutorId = nextExecutorID.get();
                String tgtExecutor = String.format("%06d", newExecutorId);
                if(newExecutorId + 1 > nextExecutorID.get()){
                    nextExecutorID.set(newExecutorId + 1);
                }
                //LOG.info("Debugging, scale out migrating partitions: " + migratingPartitions);
                return new Pair<Prescription, Map<String, Double>>(new Prescription(srcExecutor, tgtExecutor, migratingPartitions), null);
            }

            //Compare two delay vector, assume they are sorted
            private boolean vectorGreaterThan(List<Double> v1, List<Double> v2){
                Iterator<Double> i1 = v1.iterator(), i2 = v2.iterator();
                while(i1.hasNext()){
                    double t1 = i1.next(), t2 = i2.next();
                    if(t1 - 1e-9 > t2)return true;
                    if(t2 - 1e-9 > t1)return false;
                }
                return false;
            }
            //Iterate all pairs of source and targe OE, find the one minimize delay vector
            private Pair<Prescription, Map<String, Double>> tryToScaleIn(){
                LOG.info("Try to scale in");
                if(executorMapping.size() <= 1){
                    LOG.info("Not enough executor to merge");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                String minsrc = "", mintgt = "";
                List<Double> best = null;

                for(String src: executorMapping.keySet()){
                    double srcArrival = examiner.model.executorArrivalRate.get(src);
                    for(String tgt: executorMapping.keySet())
                        if(!tgt.equals(src)){
                            double tgtArrival = examiner.model.executorArrivalRate.get(tgt);
                            double tgtService = examiner.model.executorServiceRate.get(tgt);
                            //Try to migrate all partitions from src to tgt
                            if(srcArrival + tgtArrival < tgtService){
                                double estimatedLongtermDelay = 1.0/(tgtService - (srcArrival + tgtArrival));
                                List<Double> current = new ArrayList<>();
                                for(String executor: executorMapping.keySet()){
                                    if(executor.equals(src)){
                                        current.add(0.0);
                                    }else if(executor.equals(tgt)){
                                        current.add(estimatedLongtermDelay);
                                    }else{
                                        current.add(examiner.model.getLongTermDelay(executor));
                                    }
                                }
                                current.sort(Collections.reverseOrder());
                                if(best == null || vectorGreaterThan(best, current)){
                                    best = current;
                                    minsrc = src;
                                    mintgt = tgt;
                                }
                            }
                        }
                }


                if(best != null){
                    List<String> migratingPartitions = new ArrayList<>(executorMapping.get(minsrc));
                    LOG.info("Scale in! from " + minsrc + " to " + mintgt);
                    LOG.info("Migrating partitions: " + migratingPartitions);
                    HashMap<String, Double> map = new HashMap<>();
                    for(String executor: executorMapping.keySet()){
                        if(executor.equals(minsrc)){
                            map.put(minsrc, 0.0);
                        }else if(executor.equals(mintgt)){
                            map.put(mintgt, 1.0/(examiner.model.executorServiceRate.get(mintgt) - examiner.model.executorArrivalRate.get(mintgt) - examiner.model.executorArrivalRate.get(mintgt)));
                        }else{
                            map.put(executor, examiner.model.getLongTermDelay(executor));
                        }
                    }
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(minsrc, mintgt, migratingPartitions), map);
                }else {
                    LOG.info("Cannot find any scale in");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
            }

            private Pair<String, Double> findMaxLongtermDelayExecutor(Map<String, List<String>> executorMapping){
                double initialDelay = -1.0;
                String maxExecutor = "";
                for (String executor : executorMapping.keySet()) {
                    double longtermDelay = examiner.model.getLongTermDelay(executor);
                    if (longtermDelay > initialDelay) {
                        initialDelay = longtermDelay;
                        maxExecutor = executor;
                    }
                }
                return new Pair(maxExecutor, initialDelay);
            }

            private Pair<Prescription, Map<String, Double>> tryToMigrate(){
                LOG.info("Try to migrate");
                LOG.info("Migrating once based on assignment: " + executorMapping);
                if (executorMapping.size() == 0) { //No executor to move
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                //Find container with maximum delay
                Pair<String, Double> a = findMaxLongtermDelayExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if (srcExecutor.equals("")) { //No correct container
                    LOG.info("Cannot find the container that exceeds threshold");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                if (executorMapping.get(srcExecutor).size() <= 1) { //Container has only one partition
                    LOG.info("Largest delay container " + srcExecutor + " has only " + executorMapping.get(srcExecutor).size());
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
                LOG.info("Try to migrate from largest delay container " + srcExecutor);
                String bestTgtExecutor = null;
                List<Double> best = null;
                List<String> bestMigratingPartitions = null;
                for (String tgtExecutor : executorMapping.keySet())
                    if (!srcExecutor.equals(tgtExecutor)) {
                        double tgtArrivalRate = examiner.model.executorArrivalRate.get(tgtExecutor);
                        double tgtServiceRate = examiner.model.executorServiceRate.get(tgtExecutor);
                        if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                            TreeMap<Double, Set<String>> partitions = new TreeMap<>();
                            for(String partition: executorMapping.get(srcExecutor)){
                                double delay = examiner.state.getPartitionLatency(partition, examiner.state.currentTimeIndex) /
                                        ((double) examiner.state.getPartitionCompleted(partition, examiner.state.currentTimeIndex) - examiner.state.getPartitionCompleted(partition, examiner.state.currentTimeIndex - 1));
                                partitions.putIfAbsent(delay, new HashSet<>());
                                partitions.get(delay).add(partition);
                            }

                            //Debugging
                            LOG.info("Debugging, partitions' latency=" + partitions);
                            List<String> sortedPartitions = new LinkedList<>();
                            for(Map.Entry<Double, Set<String>> entry: partitions.entrySet()){
                                sortedPartitions.addAll(entry.getValue());
                            }

                            double srcArrivalRate = examiner.model.executorArrivalRate.get(srcExecutor);
                            double srcServiceRate = examiner.model.executorServiceRate.get(srcExecutor);
                            List<String> migrating = new ArrayList<>();
                            //LOG.info("Debugging, try to migrate to " + tgtExecutor + "tgt la=" + tgtArrivalRate + "tgt mu=" + tgtServiceRate);
                            for(String partition: sortedPartitions){ //Cannot migrate all partitions out?
                                double arrival = examiner.model.partitionArrivalRate.get(partition);
                                srcArrivalRate -= arrival;
                                tgtArrivalRate += arrival;
                                migrating.add(partition);
                                if(srcArrivalRate < srcServiceRate && tgtArrivalRate < tgtServiceRate){
                                    double srcDelay = 1.0 / (srcServiceRate - srcArrivalRate),
                                            tgtDelay = 1.0 / (tgtServiceRate - tgtArrivalRate);
                                    //LOG.info("Debugging, current src la=" + srcArrivalRate + " tgt la=" + tgtArrivalRate + " partitions=" + migrating);
                                    List<Double> current = new ArrayList<>();
                                    for(String executor: executorMapping.keySet()){
                                        if(executor.equals(srcExecutor)){
                                            current.add(srcDelay);
                                        }else if(executor.equals(tgtExecutor)){
                                            current.add(tgtDelay);
                                        }else{
                                            current.add(examiner.model.getLongTermDelay(executor));
                                        }
                                    }
                                    current.sort(Collections.reverseOrder()); //Delay vector is from largest to smallest.

                                    //LOG.info("Debugging, vectors=" + current);
                                    if(best == null || vectorGreaterThan(best, current)){
                                        best = current;
                                        bestTgtExecutor = tgtExecutor;
                                        bestMigratingPartitions = migrating;
                                    }
                                    if(tgtDelay > srcDelay)break;
                                }
                                if(tgtArrivalRate > tgtServiceRate)break;
                            }
                        }
                    }
                if(best == null){
                    LOG.info("Cannot find any migration");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
                LOG.info("Find best migration with delay: " + best + ", from executor " + srcExecutor + " to executor " + bestTgtExecutor + " migrating Partitions: " + bestMigratingPartitions);
                Map<String, Double> map = new HashMap<>();
                double srcArrival = examiner.model.executorArrivalRate.get(srcExecutor);
                double tgtArrival = examiner.model.executorArrivalRate.get(bestTgtExecutor);
                double srcService = examiner.model.executorServiceRate.get(srcExecutor);
                double tgtService = examiner.model.executorServiceRate.get(bestTgtExecutor);
                for(String partition: bestMigratingPartitions){
                    double arrival = examiner.model.partitionArrivalRate.get(partition);
                    srcArrival -= arrival;
                    tgtArrival += arrival;
                }
                map.put(srcExecutor, 1.0/(srcService - srcArrival));
                map.put(bestTgtExecutor, 1.0/(tgtService - tgtArrival));
                for(String executor: executorMapping.keySet()){
                    if(!executor.equals(srcExecutor) && !executor.equals(bestTgtExecutor)){
                        map.put(executor, examiner.model.getLongTermDelay(executor));
                    }
                }
                Pair<Prescription, Map<String, Double>> result = new Pair<Prescription, Map<String, Double>>(new Prescription(srcExecutor, bestTgtExecutor, bestMigratingPartitions), map);
                return result;
            }

            //Check healthisness of input delay vectors: 0 for Good, 1 for Moderate, 2 for Severe
            private int checkHealthiness(Map<String, Double> instantDelay, Map<String, Double> longtermDelay){
                boolean instantExceeded = false;
                boolean longtermExceeded = false;
                boolean both = false;
                for(Map.Entry<String, Double> entry: instantDelay.entrySet()){
                    if(entry.getValue() > alpha * latencyReq)instantExceeded = true;
                }
                for(Map.Entry<String, Double> entry: longtermDelay.entrySet()){
                    if(entry.getValue() > beta * latencyReq){
                        longtermExceeded = true;
                        if(instantDelay.get(entry.getKey()) > alpha * latencyReq)both = true;
                    }
                }
                if(both)return SERVERE;
                else if(instantExceeded  || longtermExceeded)return MODERATE;
                else return GOOD;
            }
            final static int GOOD = 0, MODERATE = 1, SERVERE = 2;
        }

        Diagnoser diagnoser = new Diagnoser();

        int healthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), examiner.getLongtermDelay());
        Prescription pres = new Prescription(null, null, null);
        LOG.info("Debugging, instant delay vector: " + examiner.getInstantDelay() + " long term delay vector: " + examiner.getLongtermDelay());
        if(isMigrating){
            LOG.info("Migration does not complete, cannot diagnose");
            return pres;
        }

        //Moderate
        if(healthiness == Diagnoser.MODERATE){
            LOG.info("Current healthiness is Moderate, do nothing");
            return pres;
        }

        //Good
        if(healthiness == Diagnoser.GOOD){
            LOG.info("Current healthiness is Good");
            //Try scale in
            Pair<Prescription, Map<String, Double>> result = diagnoser.tryToScaleIn();
            if(result.getValue() != null) {
                int thealthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), result.getValue());
                if (thealthiness == Diagnoser.GOOD) {  //Scale in OK
                    LOG.info("Scale-in is OK");
                    return result.getKey();
                }
            }
            //Do nothing
            return pres;
        }
        //Severe
        else{
            LOG.info("Current healthiness is Servere");
            Pair<Prescription, Map<String, Double>> result = diagnoser.tryToMigrate();
            //LOG.info("The result of load-balance: " + result.getValue());
            if(result.getValue() != null) {
                int thealthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), result.getValue());
                //LOG.info("If we migrating, healthiness is " + thealthiness);
                if (thealthiness == Diagnoser.MODERATE) {  //Load balance OK
                    LOG.info("Load-balance is OK");
                    return result.getKey();
                }
            }
            //Scale out
            LOG.info("Cannot load-balance, need to scale out");
            result = diagnoser.tryToScaleOut();
            return result.getKey();
        }
    }

    //Return state validity
    boolean examine(long timeIndex){
        Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
        Map<String, Long> partitionArrived =
                (HashMap<String, Long>) (metrics.get("Arrived"));
        Map<String, Long> partitionProcessed =
                (HashMap<String, Long>) (metrics.get("Processed"));
        Map<String, Double> executorUtilization =
                (HashMap<String, Double>) (metrics.get("Utilization"));
        Map<String, Boolean> partitionValid =
                (HashMap<String,Boolean>)metrics.getOrDefault("Validity", null);
        if(examiner.updateState(timeIndex, partitionArrived, partitionProcessed, executorUtilization, partitionValid, executorMapping)){
            examiner.updateModel(timeIndex, executorMapping);
            return true;
        }
        return false;
    }

    //Treatment for Samza
    void treat(Prescription pres){
        if(pres.migratingPartitions == null){
            LOG.warn("Prescription has nothing, so do no treatment");
            return ;
        }

        pendingPres = pres;
        isMigrating = true;

        LOG.info("Old mapping: " + executorMapping);
        Map<String, List<String>> newAssignment = pres.generateNewPartitionAssignment(executorMapping);
        LOG.info("Prescription : src: " + pres.source + " , tgt: " + pres.target + " , migrating: " + pres.migratingPartitions);
        LOG.info("New mapping: " + newAssignment);
        //Scale out
        if (!executorMapping.containsKey(pres.target)) {
            LOG.info("Scale out");
            //For drawing figure
            System.out.println("Migration! Scale out prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.scale(newAssignment.size(), newAssignment);
        }
        //Scale in
        else if(executorMapping.get(pres.source).size() == pres.migratingPartitions.size()) {
            LOG.info("Scale in");
            //For drawing figure
            System.out.println("Migration! Scale in prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.scale(newAssignment.size(), newAssignment);
        }
        //Load balance
        else {
            LOG.info("Load balance");
            //For drawing figure
            System.out.println("Migration! Load balance prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.remap(newAssignment);
        }
    }

    //Main logic:  examine->diagnose->treatment->sleep
    void work(long timeIndex) {
        LOG.info("Examine...");
        //Examine
        boolean stateValidity = examine(timeIndex);
        LOG.info("Diagnose...");
        if (stateValidity && !isMigrating && (timeIndex * metricsRetreiveInterval + startTime) - lastMigratedTime > migrationInterval) {
            //Diagnose
            Prescription pres = diagnose(examiner);
            if (pres.migratingPartitions != null) {
                //Treatment
                treat(pres);
            } else {
                LOG.info("Nothing to do this time.");
            }
        } else {
            if (!stateValidity) LOG.info("Current examine data is not valid, need to wait until valid");
            else if (isMigrating) LOG.info("One migration is in process");
            else LOG.info("Too close to last migration");
        }
    }

    @Override
    public synchronized void onChangeImplemented(){
        LOG.info("Migration actually deployed, try to acquire lock...");
        updateLock.lock();
        try {
            LOG.info("Lock acquired, set migrating flag to false");
            if (examiner == null) {
                LOG.warn("Examiner haven't been initialized");
            } else if (!isMigrating || pendingPres == null) {
                LOG.warn("There is no pending migration, please checkout");
            } else {
                isMigrating = false;
                lastMigratedTime = System.currentTimeMillis();
                Prescription pres = pendingPres;
                LOG.info("Migrating " + pres.migratingPartitions + " from " + pres.source + " to " + pres.target);
                //For drawing figre
                System.out.println("Change implemented at time " + System.currentTimeMillis() + " :  from " + pres.source + " to " + pres.target);

                //Scale in, remove useless information
                if(pres.migratingPartitions.size() == executorMapping.get(pres.source).size()){
                    examiner.state.executorUtilizations.remove(pres.source);
                }

                executorMapping = pres.generateNewPartitionAssignment(executorMapping);
                pendingPres = null;
            }
        }finally {
            updateLock.unlock();
            LOG.info("Migration complete, unlock");
        }
    }
}
