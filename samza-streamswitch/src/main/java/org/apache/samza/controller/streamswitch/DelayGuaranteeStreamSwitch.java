package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.controller.JobControllerListener;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Under development

public class DelayGuaranteeStreamSwitch extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(DelayGuaranteeStreamSwitch.class);
    class MigrationContext{
        Map<String, String> partitionToMove;
    }
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
            //TODO:
            //Reduce source containers' backlog
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

    NetworkCalculusModel networkCalculusModel;
    public DelayGuaranteeStreamSwitch(Config config){
        super(config);
        networkCalculusModel = new NetworkCalculusModel();
    }
    @Override
    protected boolean updateModel(long time, Map<String, Object> metrics){
        LOG.info("Updating model from metrics");
        Map<String, Long> partitionArrived = (HashMap<String, Long>)(metrics.get("PartitionArrived"));
        Map<String, Long> parititionProcessed = (HashMap<String, Long>)(metrics.get("PartitionProcessed"));
        networkCalculusModel.updateAtTime(time, partitionArrived, parititionProcessed, partitionAssignment);

        //Map<String, String> containerArrived = (HashMap<String, String>)(metrics.get(""))
        return false;
    };
    @Override
    public synchronized void onChangeImplemented(){
        if(waitForMigrationDeployed){

            waitForMigrationDeployed = false;
        }
    }
}
