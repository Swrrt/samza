package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//Under development

public class LatencyGuarantor extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyGuarantor.class);
    private long latencyReq, windowReq;
    private double alpha, beta; // Check with latencyReq * coefficient. instantDelay * alpha < req and longtermDelay * beta < req
    private boolean isValid;
    private Prescription pendingPres;
    private Examiner examiner;
    public LatencyGuarantor(Config config){
        super(config);
        latencyReq = config.getLong("streamswitch.requirement.latency", 400); //Unit: millisecond
        windowReq = config.getLong("streamswitch.requirement.window", 1000); //Unit: millisecond
        alpha = config.getDouble("streamswitch.system.alpha", 0.8);
        beta = config.getDouble("streamswitch.system.beta", 0.8);
        examiner = new Examiner();
        isValid = false;
        pendingPres = null;
    }



    class Examiner{
        class State {
            Map<String, Map<Long, Long>> partitionArrived, partitionCompleted; //Instead of actual time, use the n-th time point as key
            Map<String, Long> partitionLastValid; //Last valid state time point
            private Map<Long, Map<String, List<String>>> mappings;
            private Map<String, Map<Long, Double>> executorUtilizations;
            long beginTimeIndex;
            long currentTimeIndex;
            long storedTimeWindowSize;
            private State() {
                currentTimeIndex = 0;
                beginTimeIndex = 0;
                storedTimeWindowSize = Math.max(100, windowReq / metricsRetreiveInterval + 2);
                partitionArrived = new HashMap<>();
                partitionCompleted = new HashMap<>();
                executorUtilizations = new HashMap<>();
                partitionLastValid = new HashMap<>();
                mappings = new HashMap<>();
            }

            protected long getTimepoint(long timeIndex){
                return timeIndex * metricsRetreiveInterval;
            }

            protected Map<String, List<String>> getMapping(long n){
                if(mappings.containsKey(n))return mappings.get(n);
                else return null;
            }

            private void updatePartitionArrived(String partitionId, long n, long arrived){
                partitionArrived.putIfAbsent(partitionId, new TreeMap<>());
                partitionArrived.get(partitionId).put(n, arrived);
            }
            private void updatePartitionCompleted(String partitionId, long n, long completed){
                partitionCompleted.putIfAbsent(partitionId, new TreeMap<>());
                partitionCompleted.get(partitionId).put(n, completed);
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
            /*
            Remove old data that is useless:
                  For all partition,
                  Arrived(i) < Processed(n-1)-1?
                  and i < n - windowSize
            Only used when all partition are ready.
            */
            private void drop(){
                LOG.info("Try to drop old states");

                //Check whether all partitions are ready
                for(String partition: partitionArrived.keySet())
                    if(!partitionLastValid.containsKey(partition) || partitionLastValid.get(partition) != currentTimeIndex)return;

                //Drop partition arrived and completed data
                while(beginTimeIndex < currentTimeIndex - storedTimeWindowSize){
                    for(String partition: partitionArrived.keySet()){
                        long arrived = partitionArrived.get(partition).get(beginTimeIndex + 2);
                        long processed = partitionCompleted.get(partition).get(currentTimeIndex - (windowReq / metricsRetreiveInterval) - 2);
                        if(!(arrived + 1 < processed - 1)){
                            LOG.info("Time " +beginTimeIndex+ " is still useful for " + partition + ", cannot drop");
                            break; //If beginIndex is useful, stop pop
                        }
                        if(partitionLastValid.get(partition) <= beginTimeIndex){
                            LOG.info("Time " +beginTimeIndex+ " is the last valid state for " + partition + ", cannot drop");
                            break;
                        }
                    }
                    LOG.info("Dropping old state at time " + beginTimeIndex);
                    for(String partition: partitionArrived.keySet()){
                        partitionArrived.get(partition).remove(beginTimeIndex);
                        partitionCompleted.get(partition).remove(beginTimeIndex);
                    }
                    beginTimeIndex++;
                }

                //Drop old mappings and executor utilizations
                List<Long> removeIndex = new LinkedList<>();
                for(Long t: mappings.keySet())
                    if(t < currentTimeIndex - (int)(windowReq / metricsRetreiveInterval) - 1)removeIndex.add(t);
                LOG.info("Drop old mappings: " + removeIndex);
                for(Long t: removeIndex) {
                    mappings.remove(t);
                }
                for(String executor: executorMapping.keySet()){
                    if(executorUtilizations.containsKey(executor)){
                        removeIndex.clear();
                        for(long t: executorUtilizations.get(executor).keySet())
                            if(t < currentTimeIndex - (int)(windowReq / metricsRetreiveInterval) - 1)removeIndex.add(t);
                        LOG.info("Drop " + executor + " utilizations: " + removeIndex);
                        for(long t: removeIndex)
                            executorUtilizations.get(executor).remove(t);
                    }
                }
            }
            public long calculateArrivalTime(String partition, long r){
                for(Map.Entry<Long, Long> entry: partitionArrived.get(partition).entrySet()){
                    if(entry.getKey() > state.beginTimeIndex && r <= entry.getValue() && r > partitionArrived.get(partition).get(entry.getKey() - 1)) {
                        LOG.info("Arrival time for tuple " + r + ": t=" + entry.getKey() + " a0=" + partitionArrived.get(partition).get(entry.getKey() - 1) + " a1=" + entry.getValue());
                        return entry.getKey();
                    }
                }
                //Debugging:
                LOG.warn("Cannot find arrival time for " + partition + " r=" + r);
                LOG.warn("partition arrived:" + partitionArrived.get(partition));

                return state.beginTimeIndex;
            }

            //Only called when time n is valid, also update partitionLastValid
            private void calibratePartitionState(String partition, long n){
                long a1 = state.getPartitionArrived(partition, n);
                long c1 = state.getPartitionCompleted(partition, n);
                long n0 = partitionLastValid.getOrDefault(partition, 0l);
                if(n0 < n - 1) {
                    //LOG.info("Calibrate state for " + partition + " from time=" + n0);
                    long a0 = state.getPartitionArrived(partition, n0);
                    long c0 = state.getPartitionCompleted(partition, n0);
                    for (long i = n0 + 1; i < n; i++) {
                        long ai = (a1 - a0) * (i - n0) / (n - n0) + a0;
                        long ci = (c1 - c0) * (i - n0) / (n - n0) + c0;
                        state.partitionArrived.get(partition).put(i, ai);
                        state.partitionCompleted.get(partition).put(i, ci);
                    }
                }
                partitionLastValid.put(partition, n);
            }
            private void calibrate(Map<String, Boolean> partitionValid){
                //Calibrate mappings
                if(mappings.containsKey(currentTimeIndex)){
                    for(long t = currentTimeIndex - 1; t >= 0 && t >= currentTimeIndex - (int)(windowReq / metricsRetreiveInterval) - 1; t--){
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
                            for(long t = currentTimeIndex  - 1; t >= 0 && t >= currentTimeIndex  - (int)(windowReq / metricsRetreiveInterval) - 1; t--){
                                utilization.put(t, utilization.get(currentTimeIndex ));
                            }
                        }
                    }
                }

                //Calibrate partitions' state
                if(partitionValid == null)return ;
                for(String partition: partitionValid.keySet()){
                    if (partitionValid.get(partition)){
                        calibratePartitionState(partition, currentTimeIndex);
                    }
                }
            }

            private void updateExecutorUtilizations(String executor, long n, double util){
                if(!executorUtilizations.containsKey(executor)){
                    executorUtilizations.put(executor, new HashMap<>());
                }
                executorUtilizations.get(executor).put(n, util);
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

            public void insert(long timeIndex, Map<String, Long> taskArrived,
                               Map<String, Long> taskProcessed, Map<String, Double> utilization,
                               Map<String, List<String>> executorMapping) { //Normal update
                LOG.info("Debugging, metrics retrieved data, time: " + timeIndex + " taskArrived: "+ taskArrived + " taskProcessed: "+ taskProcessed + " assignment: " + executorMapping);
                if(currentTimeIndex == 0){ //Initialize
                    LOG.info("Initialize time point 0...");
                    for(String executor: executorMapping.keySet()){
                        for(String partition: executorMapping.get(executor)){
                            updatePartitionCompleted(partition, 0, 0);
                            updatePartitionArrived(partition, 0, 0);
                            partitionLastValid.put(partition, 0l);
                        }
                    }
                }
                currentTimeIndex = timeIndex;
                mappings.put(currentTimeIndex, executorMapping);
                LOG.info("Current time " + timeIndex);

                for(String partition: taskArrived.keySet()){
                    updatePartitionArrived(partition, currentTimeIndex, taskArrived.get(partition));
                }
                for(String partition: taskProcessed.keySet()){
                    updatePartitionCompleted(partition, currentTimeIndex, taskProcessed.get(partition));
                }
                for(String executor: utilization.keySet()){
                   double util = utilization.get(executor);
                   updateExecutorUtilizations(executor, currentTimeIndex, util);
                }
            }
        }
        //Model here is only a snapshot
        class Model {
            private State state;
            Map<String, Double> partitionArrivalRate, executorArrivalRate, serviceRate, executorInstantaneousDelay;
            public Model(){
                partitionArrivalRate = new HashMap<>();
                executorArrivalRate = new HashMap<>();
                serviceRate = new HashMap<>();
                executorInstantaneousDelay = new HashMap<>();
            }
            public void setState(State state){
                this.state = state;
            }
            //Calculate instant delay for tuples (c(n-1), c(n)]
            //Possible BUG reason: time a0 = 0 c0 = 0 is removed when dropping old state!
            private double calculatePartitionInstantDelay(String partition, long n){
                if(n <= 0)return 0;
                //No completed in n-1 ~ n
                if(state.getPartitionCompleted(partition, n - 1) == state.getPartitionCompleted(partition, n))return 0;
                if(state.getPartitionCompleted(partition, n) == 0)return 0;
                /*long ct_1 = state.getPartitionCompleted(partition, n - 1), ct = state.getPartitionCompleted(partition, n);
                long xt_1 = state.calculateArrivalTime(partition, ct_1 + 1), xt = state.calculateArrivalTime(partition, ct);
                long l = 0;
                if(xt_1 != xt){

                }else{
                    l = n - xt + 1;
                }*/
                long cn = state.getPartitionCompleted(partition, n), cn_1 = state.getPartitionCompleted(partition, n - 1);
                //m(c(n-1)+ 1), m(c(n))
                long m0 = state.calculateArrivalTime(partition, cn_1 + 1), m1 = state.calculateArrivalTime(partition, cn);
                long l = 0;
                //LOG.info("partition " + partition + " n=" + n + " cn=" + cn + " cn_1=" + cn_1 + " m0=" + m0 + " m1=" + m1);
                if(m0 != m1) {
                    long a0 = state.getPartitionArrived(partition, m0 - 1), a1 = state.getPartitionArrived(partition, m0),
                            a2 = state.getPartitionArrived(partition, m1 - 1), a3 = state.getPartitionArrived(partition, m1);
                    long M = (a1 - cn_1) * m0 - (a3 - cn) * m1;
                    long aa0 = state.calculateArrivalTime(partition, cn_1) + 1;
                    if(aa0 <= m0)aa0 = m0 + 1;
                    for (long m = aa0; m <= m1; m++) {
                        long am = state.getPartitionArrived(partition, m), am_1 = state.getPartitionArrived(partition, m - 1);
                        M += (am - am_1) * m;
                    }
                    l = (n + 1 - M / (cn - cn_1));
                //    LOG.info("a1=" + a1 + " a3=" + a3 + " aa0=" + aa0 + " M=" + M );
                }else l = n - m1 + 1;
                long T = metricsRetreiveInterval;
                long delay =  l * T;
                LOG.info("l="+ l + " delay=" + delay);
                return delay;
            }

            private double calculateExecutorInstantDelay(String executor, long n){
                double totalDelay = 0;
                long totalCompleted = 0;
                for(String partition: executorMapping.get(executor)){
                    long completed = state.getPartitionCompleted(partition, n) - state.getPartitionCompleted(partition, n - 1);
                    double delay = calculatePartitionInstantDelay(partition, n);
                    totalDelay += delay * completed;
                    //if(completed > 0)LOG.info("Debugging, partition " + partition + " delay: " + delay + " processed: " + completed);
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
                long totalServiced = 0;
                long totalTime = 0;
                long n0 = n - (int)(windowReq / metricsRetreiveInterval) + 1;
                if(n0 < 1)n0 = 1;
                for(long i = n0; i <= n; i++){
                    long time = state.getTimepoint(i) - state.getTimepoint(i - 1);
                    totalTime += time;
                    if(state.getMapping(i).containsKey(executorId)){ //Could be scaled out or scaled in
                        for(String partition: state.getMapping(i).get(executorId)) {
                            totalServiced += state.getPartitionCompleted(partition, i) - state.getPartitionCompleted(partition, i-1);
                        }
                    }
                }
                if(totalTime > 0) return totalServiced/((double)totalTime);
                return 0;
            }



            // Window average utilization

            //Window average delay
            public double getWindowExecutorInstantaneousDelay(String executorId, long n){
                double totalDelay = 0;
                long totalProcessed = 0;
                long n0 = n - (int)(windowReq / metricsRetreiveInterval) + 1;
                if(n0<1)n0 = 1;
                for(long i = n0; i <= n; i++){
                    long processed = 0;
                    if(state.getMapping(i).containsKey(executorId)){
                        for(String partition: state.getMapping(i).get(executorId)){
                            processed += state.getPartitionCompleted(partition, i) - state.getPartitionCompleted(partition, i - 1);
                        }
                    }
                    totalProcessed += processed;
                    totalDelay += calculateExecutorInstantDelay(executorId, i) * processed;
                }
                if(totalProcessed > 0) totalDelay/=totalProcessed;
                return totalDelay;
            }

            //Calculate model snapshot from state
            public void update(long n, Map<String, List<String>> executorMapping){
                LOG.info("Updating model snapshot, clear old data...");
                partitionArrivalRate.clear();
                executorArrivalRate.clear();
                serviceRate.clear();
                executorInstantaneousDelay.clear();
                for(String executor: executorMapping.keySet()){
                    double arrivalRate = 0;
                    for(String partition: executorMapping.get(executor)){
                        double t = calculatePartitionArrivalRate(partition, n - (int)(windowReq / metricsRetreiveInterval), n);
                        partitionArrivalRate.put(partition, t);
                        arrivalRate += t;
                    }
                    executorArrivalRate.put(executor, arrivalRate);
                    double util = state.getAverageExecutorUtilization(executor);
                    double mu = calculateExecutorServiceRate(executor, n);
                    if(util > 1e-9 && util <= 1){
                        mu /= util;
                    }
                    serviceRate.put(executor, mu);
                    executorInstantaneousDelay.put(executor, getWindowExecutorInstantaneousDelay(executor, n));
                }
                LOG.info("Debugging, partition arrival rate: " + partitionArrivalRate);
                LOG.info("Debugging, executor windowed service: " + serviceRate);
                LOG.info("Debugging, executor windowed delay: " + executorInstantaneousDelay);
            }
        }
        private Model model;
        private State state;
        Examiner(){
            this.state = new State();
            this.model = new Model();
            this.model.setState(this.state);
            lastMigratedTime = 0l;
        }

        private boolean checkStateValidity(Map<String, Boolean> partitionValid){
            if(partitionValid == null)return false;

            //Current we don't haven enough time slot to calculate model
            if(state.currentTimeIndex < (int)(windowReq / metricsRetreiveInterval)){
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

        private void updateState(long timeIndex, Map<String, Long> partitionArrived, Map<String, Long> partitionProcessed, Map<String, Double> executorUtilization, Map<String, Boolean> partitionValid, Map<String, List<String>> executorMapping){
            LOG.info("Updating state...");
            state.insert(timeIndex, partitionArrived, partitionProcessed, executorUtilization, executorMapping);
            state.calibrate(partitionValid);
            state.drop();

            //Debug & Statistics
            HashMap<String, Long> arrived = new HashMap<>(), completed = new HashMap<>();
            for(String partition: state.partitionArrived.keySet()) {
                arrived.put(partition, state.partitionArrived.get(partition).get(state.currentTimeIndex));
                completed.put(partition, state.partitionCompleted.get(partition).get(state.currentTimeIndex));
            }
            System.out.println("State, time " + timeIndex  + " , Partition Arrived: " + arrived);
            System.out.println("State, time " + timeIndex  + " , Partition Completed: " + completed);
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
                System.out.println("Model, time " + time  + " , Service Rate: " + model.serviceRate);
                System.out.println("Model, time " + time  + " , Instantaneous Delay: " + model.executorInstantaneousDelay);
                System.out.println("Model, time " + time  + " , Longterm Delay: " + longtermDelay);
                System.out.println("Model, time " + time  + " , Partition Arrival Rate: " + model.partitionArrivalRate);
            }
        }
        private Map<String, Double> getInstantDelay(){
            return examiner.model.executorInstantaneousDelay;
        }

        private List<Pair<String, Double>> getLongtermDelay(){
            List<Pair<String, Double>> delay = new LinkedList<>();
            for(String executor: executorMapping.keySet()){
                delay.add(new Pair(executor,model.getLongTermDelay(executor)));
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
            private Pair<Prescription, List<Pair<String, Double>>> tryToScaleOut(){
                LOG.info("Scale out by one container");
                if(executorMapping.size() <= 0){
                    LOG.info("No executor to move");
                    return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                }
                Pair<String, Double> a = findMaxLongtermDelayExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if(srcExecutor == null || srcExecutor.equals("") || executorMapping.get(srcExecutor).size() <=1){
                    LOG.info("Cannot scale out: insufficient partition to migrate");
                    return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                }

                LOG.info("Migrating out from executor " + srcExecutor);

                //Try from lightest arrival to heaviest arrival
                PriorityQueue<Pair<String, Double>> priorityQueue = new PriorityQueue<>((x,y)-> {
                    if(x.getValue() - 1e-9 > y.getValue())return 1;
                    if(y.getValue() - 1e-9 > x.getValue())return -1;
                    return x.getKey().compareTo(y.getKey());
                });

                for(String partition: executorMapping.get(srcExecutor)){
                    priorityQueue.add(new Pair(partition, examiner.model.calculatePartitionInstantDelay(partition, examiner.state.currentTimeIndex)));
                }

                List<String> partitions = new LinkedList<>();
                while(priorityQueue.size() > 0){
                    partitions.add(priorityQueue.poll().getKey());
                }
                LOG.info("Sorted partitions: " + partitions);

                double arrivalrate0 = examiner.model.executorArrivalRate.get(srcExecutor), arrivalrate1 = 0;
                //double serviceRate = examiner.model.serviceRate.get(srcExecutor); //Assume new executor has same service rate
                double best = 1e100;
                List<String> migratingPartitions = new ArrayList<>();
                for(String partition: partitions){
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
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(srcExecutor, tgtExecutor, migratingPartitions), null);
            }

            //Compare two delay vector, assume they are sorted
            private boolean vectorGreaterThan(PriorityQueue<Pair<String, Double>> v1, PriorityQueue<Pair<String, Double>> v2){
                Iterator<Pair<String,Double>> i1 = v1.iterator(), i2 = v2.iterator();
                while(i1.hasNext()){
                    double t1 = i1.next().getValue(), t2 = i2.next().getValue();
                    if(t1 - 1e-9 > t2)return true;
                    if(t2 - 1e-9 > t1)return false;
                }
                i1 = v1.iterator();
                i2 = v2.iterator();
                while(i1.hasNext()) {
                    int t = i1.next().getKey().compareTo(i2.next().getKey());
                    if(t > 0)return true;
                    if(t < 0)return false;
                }
                return false;
            }
            //Iterate all pairs of source and targe OE, find the one minimize delay vector
            private Pair<Prescription, List<Pair<String, Double>>> tryToScaleIn(){
                LOG.info("Try to scale in");
                if(executorMapping.size() <= 1){
                    LOG.info("Not enough executor to merge");
                    return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                }

                String minsrc = "", mintgt = "";
                PriorityQueue<Pair<String, Double>> best = null;

                for(String src: executorMapping.keySet()){
                    double srcArrival = examiner.model.executorArrivalRate.get(src);
                    for(String tgt: executorMapping.keySet())
                        if(!src.equals(tgt)){
                            double tgtArrival = examiner.model.executorArrivalRate.get(tgt);
                            double tgtService = examiner.model.serviceRate.get(tgt);
                            if(srcArrival + tgtArrival < tgtService){
                                double estimatedLongtermDelay = 1.0/(tgtService - (srcArrival + tgtArrival));
                                PriorityQueue<Pair<String, Double>> current = new PriorityQueue<>((x,y)-> {
                                    if(x.getValue() - 1e-9 > y.getValue())return 1;
                                    if(y.getValue() - 1e-9 > x.getValue())return -1;
                                    return x.getKey().compareTo(y.getKey());
                                });
                                for(String executor: executorMapping.keySet()){
                                    if(executor.equals(src)){
                                        current.add(new Pair(executor, 0.0));
                                    }else if(executor.equals(tgt)){
                                        current.add(new Pair(executor, estimatedLongtermDelay));
                                    }else{
                                        current.add(new Pair(executor, examiner.model.getLongTermDelay(executor)));
                                    }
                                }

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
                    return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(minsrc, mintgt, migratingPartitions), new ArrayList(best));
                }
                LOG.info("Cannot find any scale in");
                return new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
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

            private Pair<Prescription, List<Pair<String, Double>>> tryToMigrate(){
                LOG.info("Try to migrate");
                LOG.info("Migrating once based on assignment: " + executorMapping);
                if (executorMapping.size() == 0) { //No executor to move
                    Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                    return result;
                }

                //Find container with maximum delay
                Pair<String, Double> a = findMaxLongtermDelayExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if (srcExecutor.equals("")) { //No correct container
                    LOG.info("Cannot find the container that exceeds threshold");
                    Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                    return result;
                }

                if (executorMapping.get(srcExecutor).size() <= 1) { //Container has only one partition
                    LOG.info("Largest delay container " + srcExecutor + " has only " + executorMapping.get(srcExecutor).size());
                    Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                    return result;
                }
                LOG.info("Try to migrate from largest delay container " + srcExecutor);
                String bestTgtExecutor = null;
                PriorityQueue<Pair<String, Double>> best = null;
                List<String> bestMigratingPartitions = null;
                for (String tgtExecutor : executorMapping.keySet())
                    if (!srcExecutor.equals(tgtExecutor)) {
                        double tgtArrivalRate = examiner.model.executorArrivalRate.get(tgtExecutor);
                        double tgtServiceRate = examiner.model.serviceRate.get(tgtExecutor);
                        if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                            PriorityQueue<Pair<String, Double>> partitions = new PriorityQueue<>((x,y)-> {
                                if(x.getValue() - 1e-9 > y.getValue())return -1;
                                if(y.getValue() - 1e-9 > x.getValue())return 1;
                                return x.getKey().compareTo(y.getKey());
                            });

                            for(String partition: executorMapping.get(srcExecutor)){
                                partitions.add(new Pair(partition, examiner.model.calculatePartitionInstantDelay(partition, examiner.state.currentTimeIndex)));
                            }
                            //Debugging
                            for(Pair<String, Double> x:partitions){
                                LOG.info("Partition " + x.getKey() + " delay=" + x.getValue());
                            }

                            double srcArrivalRate = examiner.model.executorArrivalRate.get(srcExecutor);
                            double srcServiceRate = examiner.model.serviceRate.get(srcExecutor);
                            List<String> migrating = new ArrayList<>();
                            //LOG.info("Debugging, try to migrate to " + tgtExecutor + "tgt la=" + tgtArrivalRate + "tgt mu=" + tgtServiceRate);
                            while(partitions.size() > 1){ //Cannot migrate all partitions out?
                                Pair<String, Double> t = partitions.poll();
                                double arrival = examiner.model.partitionArrivalRate.get(t.getKey());
                                srcArrivalRate -= arrival;
                                tgtArrivalRate += arrival;
                                migrating.add(t.getKey());
                                if(srcArrivalRate < srcServiceRate && tgtArrivalRate < tgtServiceRate){
                                    double srcDelay = 1.0 / (srcServiceRate - srcArrivalRate),
                                            tgtDelay = 1.0 / (tgtServiceRate - tgtArrivalRate);
                                    //LOG.info("Debugging, current src la=" + srcArrivalRate + " tgt la=" + tgtArrivalRate + " partitions=" + migrating);
                                    PriorityQueue<Pair<String, Double>> current = new PriorityQueue<>((x,y)-> {
                                        if(x.getValue() - 1e-9 > y.getValue())return -1;
                                        if(y.getValue() - 1e-9 > x.getValue())return 1;
                                        return x.getKey().compareTo(y.getKey());
                                    });
                                    for(String executor: executorMapping.keySet()){
                                        if(executor.equals(srcExecutor)){
                                            current.add(new Pair(executor, srcDelay));
                                        }else if(executor.equals(tgtExecutor)){
                                            current.add(new Pair(executor, tgtDelay));
                                        }else{
                                            current.add(new Pair(executor, examiner.model.getLongTermDelay(executor)));
                                        }
                                    }
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
                    Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(), null);
                    return result;
                }
                LOG.info("Find best migration with delay: " + best + ", from executor " + srcExecutor + " to executor " + bestTgtExecutor + " migrating Partitions: " + bestMigratingPartitions);
                Pair<Prescription, List<Pair<String, Double>>> result = new Pair<Prescription, List<Pair<String, Double>>>(new Prescription(srcExecutor, bestTgtExecutor, bestMigratingPartitions), new ArrayList<>(best));
                return result;
            }

            //Check healthisness of input delay vectors: 0 for Good, 1 for Moderate, 2 for Severe
            private int checkHealthiness(Map<String, Double> instantDelay, List<Pair<String, Double>> longtermDelay){
                int instantExceeded = 0;
                int longtermExceeded = 0;
                int both = 0;
                for(Map.Entry<String, Double> entry: instantDelay.entrySet()){
                    if(entry.getValue() > alpha * latencyReq)instantExceeded = 1;
                }
                for(Pair<String, Double> entry: longtermDelay){
                    if(entry.getValue() > beta * latencyReq){
                        longtermExceeded = 1;
                        if(instantDelay.get(entry.getKey()) > alpha * latencyReq)both = 1;
                    }
                }
                if(both == 1)return 2;
                if(instantExceeded > 0 || longtermExceeded > 0)return 1;
                return 0;
            }
        };

        Diagnoser diagnoser = new Diagnoser();

        int healthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), examiner.getLongtermDelay());
        Prescription pres = new Prescription(null, null, null);
        LOG.info("Debugging, instant delay vector: " + examiner.getInstantDelay() + " long term delay vector: " + examiner.getLongtermDelay());
        if(isMigrating){
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
            LOG.info("Current healthiness is Good");
            //Try scale in
            Pair<Prescription, List<Pair<String, Double>>> result = diagnoser.tryToScaleIn();
            if(result.getValue() != null) {
                int thealthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), result.getValue());
                if (thealthiness == 0) {  //Scale in OK
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
            Pair<Prescription, List<Pair<String, Double>>> result = diagnoser.tryToMigrate();
            //LOG.info("The result of load-balance: " + result.getValue());
            if(result.getValue() != null) {
                int thealthiness = diagnoser.checkHealthiness(examiner.getInstantDelay(), result.getValue());
                //LOG.info("If we migrating, healthiness is " + thealthiness);
                if (thealthiness == 1) {  //Load balance OK
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

    void examine(long timeIndex){
        Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
        Map<String, Long> partitionArrived =
                (HashMap<String, Long>) (metrics.get("PartitionArrived"));
        Map<String, Long> partitionProcessed =
                (HashMap<String, Long>) (metrics.get("PartitionProcessed"));
        Map<String, Double> executorUtilization =
                (HashMap<String, Double>) (metrics.get("ExecutorUtilization"));
        Map<String, Boolean> partitionValid =
                (HashMap<String,Boolean>)metrics.getOrDefault("PartitionValid", null);
        examiner.updateState(timeIndex, partitionArrived, partitionProcessed, executorUtilization, partitionValid, executorMapping);
        isValid = examiner.checkStateValidity(partitionValid);
        if(isValid)examiner.updateModel(timeIndex, executorMapping);
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
        examine(timeIndex);
        LOG.info("Diagnose...");
        if (isValid && !isMigrating && (timeIndex * metricsRetreiveInterval + startTime) - lastMigratedTime > migrationInterval) {
            //Diagnose
            Prescription pres = diagnose(examiner);
            if (pres.migratingPartitions != null) {
                //Treatment
                treat(pres);
            } else {
                LOG.info("Nothing to do this time.");
            }
        } else {
            if (!isValid) LOG.info("Current examine data is not valid, need to wait until valid");
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
