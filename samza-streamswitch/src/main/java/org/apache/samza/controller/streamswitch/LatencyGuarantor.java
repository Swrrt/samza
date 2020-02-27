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
    public void init(OperatorControllerListener listener, List<String> executors, List<String> substreams) {
        super.init(listener, executors, substreams);
        //TODO: Initialize state/model here
        examiner.init(executorMapping);
    }

    class Examiner{
        class State {
            private Map<Long, Map<String, List<String>>> mappings;
            private Map<String, Map<Long, Double>> executorUtilizations;
            class SubstreamState{
                private Map<Long, Long> arrived, completed; //Instead of actual time, use the n-th time point as key
                private long lastValidIndex;    //Last valid state time point
                //For calculate instant delay
                private Map<Long, Long> totalLatency;
                private long arrivedIndex, remainedIndex;
                SubstreamState(){
                    arrived = new HashMap<>();
                    completed = new HashMap<>();
                    totalLatency = new HashMap<>();
                }
            }
            Map<String, SubstreamState> substreamStates;
            private long lastValidTimeIndex;
            private long currentTimeIndex;
            private State() {
                currentTimeIndex = 0;
                lastValidTimeIndex = 0;
                executorUtilizations = new HashMap<>();
                mappings = new HashMap<>();
                substreamStates = new HashMap<>();
            }

            private void init(Map<String, List<String>> executorMapping){
                LOG.info("Initialize time point 0...");
                for (String executor : executorMapping.keySet()) {
                    for (String substream : executorMapping.get(executor)) {
                        SubstreamState substreamState = new SubstreamState();
                        substreamState.arrived.put(0l, 0l);
                        substreamState.completed.put(0l, 0l);
                        substreamState.lastValidIndex = 0l;
                        substreamState.arrivedIndex = 1l;
                        substreamState.remainedIndex = 0l;
                        substreamStates.put(substream, substreamState);
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
            public long getSubstreamArrived(String substream, long n){
                long arrived = 0;
                if(substreamStates.containsKey(substream)){
                    arrived = substreamStates.get(substream).arrived.getOrDefault(n, 0l);
                }
                return arrived;
            }
            public long getSubstreamCompleted(String substream, long n){
                long completed = 0;
                if(substreamStates.containsKey(substream)){
                    completed = substreamStates.get(substream).completed.getOrDefault(n, 0l);
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


            public long getSubstreamLatency(String substream, long timeIndex){
                long latency = 0;
                if(substreamStates.containsKey(substream)){
                    latency = substreamStates.get(substream).totalLatency.getOrDefault(timeIndex, 0l);
                }
                return latency;
            }

            //Calculate the total latency in new slot. Drop useless data during calculation.
            private void calculateSubstreamLatency(String substream, long timeIndex){
                //Calculate total latency and # of tuples in current time slots
                SubstreamState substreamState = substreamStates.get(substream);
                long arrivalIndex = substreamState.arrivedIndex;
                long complete = getSubstreamCompleted(substream, timeIndex);
                long lastComplete = getSubstreamCompleted(substream, timeIndex - 1);
                long arrived = getSubstreamArrived(substream, arrivalIndex);
                long lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
                long totalDelay = 0;
                while(arrivalIndex <= timeIndex && lastArrived < complete){
                    if(arrived > lastComplete){ //Should count into this slot
                        long number = Math.min(complete, arrived) - Math.max(lastComplete, lastArrived);
                        totalDelay += number * (timeIndex - arrivalIndex + 1);
                    }
                    arrivalIndex++;
                    arrived = getSubstreamArrived(substream, arrivalIndex);
                    lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
                }
                arrivalIndex--;
                substreamState.totalLatency.put(timeIndex, totalDelay);
                if(substreamState.totalLatency.containsKey(timeIndex - windowReq)){
                    substreamState.totalLatency.remove(timeIndex - windowReq);
                }
                substreamState.arrivedIndex = arrivalIndex;
            }

            private void calculate(long timeIndex){
                for(long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
                    //Calculate latency from last valid time index
                    for(String substream: substreamStates.keySet()){
                        calculateSubstreamLatency(substream, index);
                    }

                    //Debugging
                    Map<String, Long> substreamArrivedIndex = new HashMap<>();
                    for(String substream: substreamStates.keySet()){
                        substreamArrivedIndex.put(substream, substreamStates.get(substream).arrivedIndex);
                    }
                    LOG.info("Debugging, time=" + index + " arrival index=" + substreamArrivedIndex);
                }
            }

            private void drop(long timeIndex){
                long totalSize = 0;
                //Drop arrived
                for (String substream : substreamStates.keySet()) {
                    long remainedIndex = substreamStates.get(substream).remainedIndex;
                    while (remainedIndex < substreamStates.get(substream).arrivedIndex - 1 && remainedIndex < timeIndex - windowReq) {
                        substreamStates.get(substream).arrived.remove(remainedIndex);
                        remainedIndex++;
                    }
                    substreamStates.get(substream).remainedIndex = remainedIndex;
                    totalSize += substreamStates.get(substream).arrivedIndex - remainedIndex + 1;
                }

                //Drop completed, utilization, mappings. These are fixed window
                for(long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
                    for (String executor : executorMapping.keySet()) {
                        for (String substream : executorMapping.get(executor)) {
                            //Drop completed
                            if (substreamStates.get(substream).completed.containsKey(index - windowReq - 1)) {
                                substreamStates.get(substream).completed.remove(index - windowReq - 1);
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
                LOG.info("Useless state dropped, current arrived size: " + totalSize + " mapping size: " + mappings.size());
            }

            //Only called when time n is valid, also update substreamLastValid
            private void calibrateSubstream(String substream, long timeIndex){
                long n0 = substreamStates.get(substream).lastValidIndex;
                substreamStates.get(substream).lastValidIndex = timeIndex;
                if(n0 < timeIndex - 1) {
                    //LOG.info("Calibrate state for " + substream + " from time=" + n0);
                    long a0 = state.getSubstreamArrived(substream, n0);
                    long c0 = state.getSubstreamCompleted(substream, n0);
                    long a1 = state.getSubstreamArrived(substream, timeIndex);
                    long c1 = state.getSubstreamCompleted(substream, timeIndex);
                    for (long i = n0 + 1; i < timeIndex; i++) {
                        long ai = (a1 - a0) * (i - n0) / (timeIndex - n0) + a0;
                        long ci = (c1 - c0) * (i - n0) / (timeIndex - n0) + c0;
                        state.substreamStates.get(substream).arrived.put(i, ai);
                        state.substreamStates.get(substream).completed.put(i, ci);
                    }
                }
            }

            //Calibrate whole state including mappings and utilizations
            private void calibrate(Map<String, Boolean> substreamValid){
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

                //Calibrate substreams' state
                if(substreamValid == null)return ;
                for(String substream: substreamValid.keySet()){
                    if (substreamValid.get(substream)){
                        calibrateSubstream(substream, currentTimeIndex);
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

                for(String substream: taskArrived.keySet()){
                    substreamStates.get(substream).arrived.put(currentTimeIndex, taskArrived.get(substream));
                }
                for(String substream: taskProcessed.keySet()){
                    substreamStates.get(substream).completed.put(currentTimeIndex, taskProcessed.get(substream));
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
            Map<String, Double> substreamArrivalRate, executorArrivalRate, executorServiceRate, executorInstantaneousDelay; //Longterm delay could be calculated from arrival rate and service rate
            public Model(State state){
                substreamArrivalRate = new HashMap<>();
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

            private double calculateSubstreamArrivalRate(String substream, long n0, long n1){
                if(n0 < 0)n0 = 0;
                long time = state.getTimepoint(n1) - state.getTimepoint(n0);
                long totalArrived = state.getSubstreamArrived(substream, n1) - state.getSubstreamArrived(substream, n0);
                if(time > 1e-9)return totalArrived / ((double)time);
                return 0.0;
            }

            // Calculate window service rate of n - beta ~ n (exclude n - beta)
            private double calculateExecutorServiceRate(String executorId, long n){
                long totalCompleted = 0;
                long totalTime = 0;
                long n0 = n - windowReq + 1;
                if(n0 < 1)n0 = 1;
                totalTime = state.getTimepoint(n) - state.getTimepoint(n0 - 1);
                for(long i = n0; i <= n; i++){
                    if(state.getMapping(i).containsKey(executorId)){ //Could be scaled out or scaled in
                        for(String substream: state.getMapping(i).get(executorId)) {
                            totalCompleted += state.getSubstreamCompleted(substream, i) - state.getSubstreamCompleted(substream, i - 1);
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
                        for(String substream: state.getMapping(i).get(executorId)){
                            totalCompleted += state.getSubstreamCompleted(substream, i) - state.getSubstreamCompleted(substream, i - 1);
                            totalDelay += state.getSubstreamLatency(substream, i);
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
                substreamArrivalRate.clear();
                executorArrivalRate.clear();
                executorInstantaneousDelay.clear();
                Map<String, Double> utils = new HashMap<>();
                for(String executor: executorMapping.keySet()){
                    double arrivalRate = 0;
                    for(String substream: executorMapping.get(executor)){
                        double t = calculateSubstreamArrivalRate(substream, timeIndex - windowReq, timeIndex);
                        substreamArrivalRate.put(substream, t);
                        arrivalRate += t;
                    }
                    executorArrivalRate.put(executor, arrivalRate);
                    double util = state.getAverageExecutorUtilization(executor);
                    utils.put(executor, util);
                    double mu = calculateExecutorServiceRate(executor, timeIndex);
                    if(util > 0.5 && util <= 1){ //Only update true service rate (capacity when utilization > 50%, so the error will be smaller)
                        mu /= util;
                        executorServiceRate.put(executor, mu);
                    }
                    if(!executorServiceRate.containsKey(executor))executorServiceRate.put(executor, arrivalRate * 1.5); //Only calculate the service rate when no historical service rate
                    executorInstantaneousDelay.put(executor, calculateExecutorInstantaneousDelay(executor, timeIndex));
                }
                //Debugging
                LOG.info("Debugging, avg utilization: " + utils);
                LOG.info("Debugging, partition arrival rate: " + substreamArrivalRate);
                LOG.info("Debugging, executor avg service rate: " + executorServiceRate);
                LOG.info("Debugging, executor avg delay: " + executorInstantaneousDelay);
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

        private boolean checkValidity(Map<String, Boolean> substreamValid){
            if(substreamValid == null)return false;

            //Current we don't haven enough time slot to calculate model
            if(state.currentTimeIndex < windowReq){
                LOG.info("Current time slots number is smaller than beta, not valid");
                return false;
            }
            //Substream Metrics Valid
            for(String executor: executorMapping.keySet()) {
                for (String substream : executorMapping.get(executor)) {
                    if (!substreamValid.containsKey(substream) || !substreamValid.get(substream)) {
                        LOG.info(substream + "'s metrics is not valid");
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
            }
            return true;
        }


        private boolean updateState(long timeIndex, Map<String, Long> substreamArrived, Map<String, Long> substreamProcessed, Map<String, Double> executorUtilization, Map<String, Boolean> substreamValid, Map<String, List<String>> executorMapping){
            LOG.info("Updating state...");
            state.insert(timeIndex, substreamArrived, substreamProcessed, executorUtilization, executorMapping);
            state.calibrate(substreamValid);

            //Debug & Statistics
            HashMap<String, Long> arrived = new HashMap<>(), completed = new HashMap<>();
            for(String substream: state.substreamStates.keySet()) {
                arrived.put(substream, state.substreamStates.get(substream).arrived.get(state.currentTimeIndex));
                completed.put(substream, state.substreamStates.get(substream).completed.get(state.currentTimeIndex));
            }
            System.out.println("State, time " + timeIndex  + " , Partition Arrived: " + arrived);
            System.out.println("State, time " + timeIndex  + " , Partition Completed: " + completed);

            if(checkValidity(substreamValid)){
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
                System.out.println("Model, time " + time  + " , Partition Arrival Rate: " + model.substreamArrivalRate);
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
        List<String> migratingSubstreams;
        Prescription(){
            migratingSubstreams = null;
        }
        Prescription(String source, String target, List<String> migratingSubstreams){
            this.source = source;
            this.target = target;
            this.migratingSubstreams = migratingSubstreams;
        }
        Map<String, List<String>> generateNewSubstreamAssignment(Map<String, List<String>> oldAssignment){
            Map<String, List<String>> newAssignment = new HashMap<>();
            for(String executor: oldAssignment.keySet()){
                List<String> substreams = new ArrayList<>(oldAssignment.get(executor));
                newAssignment.put(executor, substreams);
            }
            if (!newAssignment.containsKey(target)) newAssignment.put(target, new LinkedList<>());
            for (String substream : migratingSubstreams) {
                newAssignment.get(source).remove(substream);
                newAssignment.get(target).add(substream);
            }
            //For scale in
            if (newAssignment.get(source).size() == 0) newAssignment.remove(source);
            return newAssignment;
        }
    }
    private Prescription diagnose(Examiner examiner){

        class Diagnoser { //Algorithms and utility methods
            //Get largest long-term latency executor
            private Pair<String, Double> getWorstExecutor(Map<String, List<String>> executorMapping){
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
            //Calculate healthisness of input delay vectors: 0 for Good, 1 for Moderate, 2 for Severe
            private int getHealthiness(Map<String, Double> instantDelay, Map<String, Double> longtermDelay){
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

            // Find the subset which minimizes delay by greedy:
            // Sort arrival rate from largest to smallest, cut them in somewhere
            private Pair<Prescription, Map<String, Double>> scaleOut(){
                LOG.info("Scale out by one container");
                if(executorMapping.size() <= 0){
                    LOG.info("No executor to move");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
                Pair<String, Double> a = getWorstExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if(srcExecutor == null || srcExecutor.equals("") || executorMapping.get(srcExecutor).size() <=1){
                    LOG.info("Cannot scale out: insufficient substream to migrate");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                LOG.info("Migrating out from executor " + srcExecutor);

                //Try from smallest latency to largest latency
                TreeMap<Double, Set<String>> substreams = new TreeMap<>();

                for(String substream: executorMapping.get(srcExecutor)){
                    double delay = examiner.state.getSubstreamLatency(substream, examiner.state.currentTimeIndex) /
                            ((double) examiner.state.getSubstreamCompleted(substream, examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(substream, examiner.state.currentTimeIndex - 1));
                    substreams.putIfAbsent(delay, new HashSet());
                    substreams.get(delay).add(substream);
                }
                //Debugging
                LOG.info("Debugging, substreams' latency=" + substreams);

                List<String> sortedSubstreams = new LinkedList<>();
                for(Map.Entry<Double, Set<String>> entry: substreams.entrySet()){
                    sortedSubstreams.addAll(entry.getValue());
                }

                double arrivalrate0 = examiner.model.executorArrivalRate.get(srcExecutor), arrivalrate1 = 0;
                //double executorServiceRate = examiner.model.executorServiceRate.get(srcExecutor); //Assume new executor has same service rate
                double best = 1e100;
                List<String> migratingSubstreams = new ArrayList<>();
                for(String substream: sortedSubstreams){
                    double arrival = examiner.model.substreamArrivalRate.get(substream);
                    arrivalrate0 -= arrival;
                    arrivalrate1 += arrival;

                    if(Math.max(arrivalrate0, arrivalrate1) < best){
                        best = Math.max(arrivalrate0, arrivalrate1);
                        migratingSubstreams.add(substream);
                    }
                    if(arrivalrate0 < arrivalrate1)break;
                }
                long newExecutorId = nextExecutorID.get();
                String tgtExecutor = String.format("%06d", newExecutorId);
                if(newExecutorId + 1 > nextExecutorID.get()){
                    nextExecutorID.set(newExecutorId + 1);
                }
                //LOG.info("Debugging, scale out migrating substreams: " + migratingSubstreams);
                return new Pair<Prescription, Map<String, Double>>(new Prescription(srcExecutor, tgtExecutor, migratingSubstreams), null);
            }

            //Iterate all pairs of source and targe OE, find the one minimize delay vector
            private Pair<Prescription, Map<String, Double>> scaleIn(){
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
                            //Try to migrate all substreams from src to tgt
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
                    List<String> migratingSubstreams = new ArrayList<>(executorMapping.get(minsrc));
                    LOG.info("Scale in! from " + minsrc + " to " + mintgt);
                    LOG.info("Migrating partitions: " + migratingSubstreams);
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
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(minsrc, mintgt, migratingSubstreams), map);
                }else {
                    LOG.info("Cannot find any scale in");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
            }

            private Pair<Prescription, Map<String, Double>> balanceLoad(){
                LOG.info("Try to migrate");
                LOG.info("Migrating once based on assignment: " + executorMapping);
                if (executorMapping.size() == 0) { //No executor to move
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                //Find container with maximum delay
                Pair<String, Double> a = getWorstExecutor(executorMapping);
                String srcExecutor = a.getKey();
                if (srcExecutor.equals("")) { //No correct container
                    LOG.info("Cannot find the container that exceeds threshold");
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }

                if (executorMapping.get(srcExecutor).size() <= 1) { //Container has only one substream
                    LOG.info("Largest delay container " + srcExecutor + " has only " + executorMapping.get(srcExecutor).size());
                    return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
                }
                LOG.info("Try to migrate from largest delay container " + srcExecutor);
                String bestTgtExecutor = null;
                List<Double> best = null;
                List<String> bestMigratingSubstreams = null;
                for (String tgtExecutor : executorMapping.keySet())
                    if (!srcExecutor.equals(tgtExecutor)) {
                        double tgtArrivalRate = examiner.model.executorArrivalRate.get(tgtExecutor);
                        double tgtServiceRate = examiner.model.executorServiceRate.get(tgtExecutor);
                        if (tgtArrivalRate < tgtServiceRate - 1e-9) {
                            TreeMap<Double, Set<String>> substreams = new TreeMap<>();
                            for(String substream: executorMapping.get(srcExecutor)){
                                double delay = examiner.state.getSubstreamLatency(substream, examiner.state.currentTimeIndex) /
                                        ((double) examiner.state.getSubstreamCompleted(substream, examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(substream, examiner.state.currentTimeIndex - 1));
                                substreams.putIfAbsent(delay, new HashSet<>());
                                substreams.get(delay).add(substream);
                            }

                            //Debugging
                            LOG.info("Debugging, substreams' latency=" + substreams);
                            List<String> sortedSubstreams = new LinkedList<>();
                            for(Map.Entry<Double, Set<String>> entry: substreams.entrySet()){
                                sortedSubstreams.addAll(entry.getValue());
                            }

                            double srcArrivalRate = examiner.model.executorArrivalRate.get(srcExecutor);
                            double srcServiceRate = examiner.model.executorServiceRate.get(srcExecutor);
                            List<String> migrating = new ArrayList<>();
                            //LOG.info("Debugging, try to migrate to " + tgtExecutor + "tgt la=" + tgtArrivalRate + "tgt mu=" + tgtServiceRate);
                            for(String substream: sortedSubstreams){ //Cannot migrate all substreams out?
                                double arrival = examiner.model.substreamArrivalRate.get(substream);
                                srcArrivalRate -= arrival;
                                tgtArrivalRate += arrival;
                                migrating.add(substream);
                                if(srcArrivalRate < srcServiceRate && tgtArrivalRate < tgtServiceRate){
                                    double srcDelay = 1.0 / (srcServiceRate - srcArrivalRate),
                                            tgtDelay = 1.0 / (tgtServiceRate - tgtArrivalRate);
                                    //LOG.info("Debugging, current src la=" + srcArrivalRate + " tgt la=" + tgtArrivalRate + " substreams=" + migrating);
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
                                        bestMigratingSubstreams = migrating;
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
                LOG.info("Find best migration with delay: " + best + ", from executor " + srcExecutor + " to executor " + bestTgtExecutor + " migrating Partitions: " + bestMigratingSubstreams);
                Map<String, Double> map = new HashMap<>();
                double srcArrival = examiner.model.executorArrivalRate.get(srcExecutor);
                double tgtArrival = examiner.model.executorArrivalRate.get(bestTgtExecutor);
                double srcService = examiner.model.executorServiceRate.get(srcExecutor);
                double tgtService = examiner.model.executorServiceRate.get(bestTgtExecutor);
                for(String substream: bestMigratingSubstreams){
                    double arrival = examiner.model.substreamArrivalRate.get(substream);
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
                Pair<Prescription, Map<String, Double>> result = new Pair<Prescription, Map<String, Double>>(new Prescription(srcExecutor, bestTgtExecutor, bestMigratingSubstreams), map);
                return result;
            }

            final static int GOOD = 0, MODERATE = 1, SERVERE = 2;
        }

        Diagnoser diagnoser = new Diagnoser();

        int healthiness = diagnoser.getHealthiness(examiner.getInstantDelay(), examiner.getLongtermDelay());
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
            Pair<Prescription, Map<String, Double>> result = diagnoser.scaleIn();
            if(result.getValue() != null) {
                int thealthiness = diagnoser.getHealthiness(examiner.getInstantDelay(), result.getValue());
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
            Pair<Prescription, Map<String, Double>> result = diagnoser.balanceLoad();
            //LOG.info("The result of load-balance: " + result.getValue());
            if(result.getValue() != null) {
                int thealthiness = diagnoser.getHealthiness(examiner.getInstantDelay(), result.getValue());
                //LOG.info("If we migrating, healthiness is " + thealthiness);
                if (thealthiness == Diagnoser.MODERATE) {  //Load balance OK
                    LOG.info("Load-balance is OK");
                    return result.getKey();
                }
            }
            //Scale out
            LOG.info("Cannot load-balance, need to scale out");
            result = diagnoser.scaleOut();
            return result.getKey();
        }
    }

    //Return state validity
    boolean examine(long timeIndex){
        Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
        Map<String, Long> substreamArrived =
                (HashMap<String, Long>) (metrics.get("Arrived"));
        Map<String, Long> substreamProcessed =
                (HashMap<String, Long>) (metrics.get("Processed"));
        Map<String, Double> executorUtilization =
                (HashMap<String, Double>) (metrics.get("Utilization"));
        Map<String, Boolean> substreamValid =
                (HashMap<String,Boolean>)metrics.getOrDefault("Validity", null);
        if(examiner.updateState(timeIndex, substreamArrived, substreamProcessed, executorUtilization, substreamValid, executorMapping)){
            examiner.updateModel(timeIndex, executorMapping);
            return true;
        }
        return false;
    }

    //Treatment for Samza
    void treat(Prescription pres){
        if(pres.migratingSubstreams == null){
            LOG.warn("Prescription has nothing, so do no treatment");
            return ;
        }

        pendingPres = pres;
        isMigrating = true;

        LOG.info("Old mapping: " + executorMapping);
        Map<String, List<String>> newAssignment = pres.generateNewSubstreamAssignment(executorMapping);
        LOG.info("Prescription : src: " + pres.source + " , tgt: " + pres.target + " , migrating: " + pres.migratingSubstreams);
        LOG.info("New mapping: " + newAssignment);
        //Scale out
        if (!executorMapping.containsKey(pres.target)) {
            LOG.info("Scale out");
            //For drawing figure
            System.out.println("Migration! Scale out prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.scale(newAssignment.size(), newAssignment);
        }
        //Scale in
        else if(executorMapping.get(pres.source).size() == pres.migratingSubstreams.size()) {
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
            if (pres.migratingSubstreams != null) {
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
                LOG.info("Migrating " + pres.migratingSubstreams + " from " + pres.source + " to " + pres.target);
                //For drawing figre
                System.out.println("Change implemented at time " + (System.currentTimeMillis() - startTime)/metricsRetreiveInterval + " :    from " + pres.source + " to " + pres.target);

                //Scale in, remove useless information
                if(pres.migratingSubstreams.size() == executorMapping.get(pres.source).size()){
                    examiner.state.executorUtilizations.remove(pres.source);
                    examiner.model.executorServiceRate.remove(pres.source);
                }
                executorMapping = pres.generateNewSubstreamAssignment(executorMapping);
                pendingPres = null;
            }
        }finally {
            updateLock.unlock();
            LOG.info("Migration complete, unlock");
        }
    }
}
