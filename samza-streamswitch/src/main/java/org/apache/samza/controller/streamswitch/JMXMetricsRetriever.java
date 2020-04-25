package org.apache.samza.controller.streamswitch;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.net.URL;
import java.net.URLConnection;
import java.text.NumberFormat;
import java.util.*;


//Under development

/*
    Retrieve containers' JMX port number information from logs
    Connect to JMX server accordingly
*/

public class JMXMetricsRetriever implements StreamSwitchMetricsRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.samza.controller.streamswitch.JMXMetricsRetriever.class);
    static class YarnLogRetriever{
        //String YARNHomePage;
        //String appId;
        protected YarnLogRetriever(){
            //YARNHomePage = config.get("yarn.web.address");
        }
        //Retrieve corresponding appid
        protected String retrieveAppId(String YARNHomePage, String jobname){
            String newestAppId = null;
            String appPrefix = "[\"<a href='/cluster/app/application_";
            URLConnection connection = null;
            try {
                String url = "http://" + YARNHomePage + "/cluster/apps";
                //LOG.info("Try to retrieve AppId from : " +  url);
                connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.startsWith(appPrefix) && content.split(",")[2].equals("\"" + jobname + "\"")){
                        content = content.substring(appPrefix.length(), appPrefix.length() + 18);
                        if(newestAppId == null || content.compareTo(newestAppId) > 0){
                            newestAppId = content;
                        }
                    }
                }
                //LOG.info("Retrieved newest AppId is : " + newestAppId);
            }catch (Exception e){
                LOG.info("Exception happened when retrieve AppIds, exception : " + e);
            }
            return newestAppId;
        }
        protected List<String> retrieveContainersAddress(String YARNHomePage, String appId){
            List<String> containerAddress = new LinkedList<>();
            String url = "http://" + YARNHomePage + "/cluster/appattempt/appattempt_" + appId + "_000001";
            try{
                //LOG.info("Try to retrieve containers' address from : " + url);
                URLConnection connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    String [] contents = content.split(",");
                    if(contents.length >= 4 && contents[0].startsWith("[\"<a href='")){
                        String address = contents[3].split("'")[1];
                        containerAddress.add(address);
                    }
                }
            }catch (Exception e){
                LOG.info("Exception happened when retrieve containers address : " + e);
            }
            return containerAddress;
        }
        protected void retrieveContainerJMX(Map<String, String> containerJMX, List<String> containerAddress){
            for(String address: containerAddress){
                try {
                    String url = address;
                    //LOG.info("Try to retrieve container's log for JMX from url: " + url);
                    URLConnection connection = new URL(url).openConnection();
                    Scanner scanner = new Scanner(connection.getInputStream());
                    scanner.useDelimiter("\n");
                    while(scanner.hasNext()){
                        String content = scanner.next().trim();
                        if(content.startsWith("<a href=\"/node/containerlogs/container")){
                            int in = content.indexOf("samza-container-");
                            if(in != -1){
                                int ind = content.indexOf(".log", in);
                                if(NumberUtils.isNumber(content.substring(in + 16, ind))){
                                    //String caddress = address +"/stdout/?start=0";        //Read jmx url from stdout
                                    String caddress = address + "/samza-container-" + content.substring(in + 16, ind) + "-startup.log/?start=-80000";  //Read jmx url from startup.log
                                    List<String> ret = retrieveContainerJMX(caddress);
                                    if(ret == null){ //Cannot retrieve JMXRMI for some reason
                                        LOG.info("Cannot retrieve container's JMX from : " + caddress + ", report error");
                                    }else {
                                        //LOG.info("container's JMX: " + ret);
                                        String host = url.split("[\\:]")[1].substring(2);
                                        String jmxRMI = ret.get(1).replaceAll("localhost", host);
                                        containerJMX.put(ret.get(0), jmxRMI);
                                        ret.clear();
                                    }
                                }
                            }
                        }
                    }
                    scanner.close();
                }catch (Exception e){
                    LOG.info("Exception happened when retrieve containers' JMX address : " + e);
                }
            }
        }
        protected List<String> retrieveContainerJMX(String address){
            String containerId = null, JMXaddress = null;
            try{
                //LOG.info("Try to retrieve container's JMXRMI from url: " + url);
                URLConnection connection = new URL(address).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.contains("[id=")){
                        int i = content.indexOf("[id=")+4;
                        containerId = content.substring(i, i+6);
                    }else if(content.length() < 500 && content.contains("url=service:")){
                        int i = content.indexOf("url=service") + 4;
                        JMXaddress = content.substring(i);
                    }
                    /*if(containerId!=null && JMXaddress != null){
                        return new DefaultMapEntry(containerId, JMXaddress);
                    }*/
                }
            }catch (Exception e){
                LOG.info("Exception happened when retrieve container's address : " + e);
            }
            if(containerId != null && JMXaddress != null){
                List<String> ret = new LinkedList<>();
                ret.add(containerId);
                ret.add(JMXaddress);
                return ret;
            }
            LOG.info("Warning, cannot find container's JMXRMI");
            return null;
        }
        // TODO: in 1.0.0, checkpoint offsets are available in OffsetManagerMetrics
        protected Map<String, HashMap<String, Long>> retrieveCheckpointOffsets(List<String> containerAddress, List<String> topics) {
            Map<String, HashMap<String, Long>> checkpointOffsets = new HashMap<>();
            for (String address : containerAddress) {
                try {
                    String url = address;
                    //LOG.info("Try to retrieve container's log from url: " + url);
                    URLConnection connection = new URL(url).openConnection();
                    Scanner scanner = new Scanner(connection.getInputStream());
                    scanner.useDelimiter("\n");
                    while (scanner.hasNext()) {
                        String content = scanner.next().trim();
                        if (content.startsWith("<a href=\"/node/containerlogs/container")) {
                            int in = content.indexOf("samza-container-");
                            if (in != -1) {
                                int ind = content.indexOf(".log", in);
                                if (NumberUtils.isNumber(content.substring(in + 16, ind))) {
                                    String caddress = address + "/" + content.substring(in, ind) + ".log/?start=0";
                                    for(String topic: topics) {
                                        if(!checkpointOffsets.containsKey(topic)){
                                            checkpointOffsets.put(topic, new HashMap<>());
                                        }
                                        Map<String, Long> ret = retrieveCheckpointOffset(caddress, topic);
                                        if (ret == null) { //Cannot retrieve JMXRMI for some reason
                                            LOG.info("Cannot retrieve container " + content.substring(in, ind) + " 's checkpoint, report error");
                                        } else {
                                            //LOG.info("container's JMX: " + ret);
                                            for (String partition : ret.keySet()) {
                                                long value = checkpointOffsets.get(topic).getOrDefault(partition, -1l);
                                                long v1 = ret.get(partition);
                                                if (value < v1) value = v1;
                                                checkpointOffsets.get(topic).put(partition, value);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.info("Exception happened when retrieve containers' JMX address : " + e);
                }
            }
            return checkpointOffsets;
        }
        protected Map<String, Long> retrieveCheckpointOffset(String address, String topic){
            Map<String, Long> checkpointOffset = new HashMap<>();
            try{
                String url = address;
                //LOG.info("Try to retrieve container's offset from url: " + url);
                URLConnection connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.contains("Checkpointed offset is currently ")){
                        int x = content.indexOf("[kafka, " + topic + ", ");
                        x = content.indexOf(',', x+1);
                        x = content.indexOf(',', x+1) + 2;
                        String partition = content.substring(x, content.indexOf(']', x));
                        x = content.indexOf("currently ");
                        x = content.indexOf(' ', x) + 1;
                        String value = content.substring(x, content.indexOf(' ', x));
                        checkpointOffset.put(partition, Long.parseLong(value));
                    }
                    /*if(containerId!=null && JMXaddress != null){
                        return new DefaultMapEntry(containerId, JMXaddress);
                    }*/
                }
            }catch (Exception e){
                LOG.info("Exception happened when retrieve container's address : " + e);
            }
            return checkpointOffset;
        }
    }
    private static class JMXclient{
        JMXclient(){
        }
        private boolean isWaterMark(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").contains("-high-watermark") && !name.getKeyProperty("name").contains("-messages-behind-high-watermark")
                    && !name.getKeyProperty("name").contains("window-count") && !name.getKeyProperty("name").contains(topic + "-changelog-")
                    && name.getKeyProperty("type").startsWith("samza-container-"); //For join operator
        }
        private boolean isCheckpoint(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.checkpoint.OffsetManagerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").endsWith("-loaded-checkpointed-offset")
                    && !name.getKeyProperty("name").contains("window-count") && !name.getKeyProperty("name").contains(topic + "-changelog-")
                    && name.getKeyProperty("type").startsWith("samza-container-"); //For join operator
        }
        private boolean isNextOffset(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").contains("-offset-change")
                    && !name.getKeyProperty("name").contains("window-count") && !name.getKeyProperty("name").contains(topic + "-changelog-");
        }

        private boolean isActuallyProcessed(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.TaskInstanceMetrics") && name.getKeyProperty("name").equals("messages-actually-processed");
        }

        private boolean isExecutorRunning(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.SamzaContainerMetrics") && name.getKeyProperty("name").equals("is-running");
        }

        private boolean isExecutorUtilization(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.SamzaContainerMetrics") && name.getKeyProperty("name").equals("average-utilization");
        }

        private boolean isProcessCpuUsage(ObjectName name){
            return name.getDomain().equals("org.apache.samza.metrics.JvmMetrics") && name.getKeyProperty("name").equals("process-cpu-usage");
        }

        private boolean isSystemCpuUsage(ObjectName name){
            return name.getDomain().equals("org.apache.samza.metrics.JvmMetrics") && name.getKeyProperty("name").equals("system-cpu-usage");
        }

        private boolean isExecutorServiceRate(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.SamzaContainerMetrics") && name.getKeyProperty("name").equals("service-rate");
        }


        /*
            Metrics format:
            <Metrics Type, Object>
         */
        protected Map<String, Object> retrieveMetrics(String containerId, List<String> topics, String url){
            Map<String, Object> metrics = new HashMap<>();
            //LOG.info("Try to retrieve metrics from " + url);
            JMXConnector jmxc = null;

            try{
                JMXServiceURL jmxServiceURL = new JMXServiceURL(url);
                //LOG.info("Connecting JMX server...");
                jmxc = JMXConnectorFactory.connect(jmxServiceURL, null);
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

                //Executor Utilization
                long value = -1;
                long time = System.currentTimeMillis();
                try {
                    Object os = mbsc.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), "ProcessCpuTime");
                    value = (long) os;
                    //LOG.info("Retrieved " + containerId + " ProcessCPUTime: " + os.toString());
                } catch (Exception e){
                    LOG.info("Exception when retrieving processCPUTime");
                }
                if(value == -1){
                    LOG.info("Executor CPU process time unavailable");
                }else{
                    metrics.put("ProcessCPUTime", value);
                    metrics.put("Time", time);
                }
                HashMap<String, String> partitionProcessed = new HashMap<>();
                HashMap<String, HashMap<String,String>> partitionWatermark = new HashMap<>();
                HashMap<String, HashMap<String,String>> partitionCheckpoint = new HashMap<>();
                //metrics.put("PartitionArrived", partitionArrived);
                metrics.put("PartitionWatermark", partitionWatermark);
                //metrics.put("PartitionNextOffset", partitionNextOffset);
                metrics.put("PartitionProcessed", partitionProcessed);
                metrics.put("PartitionCheckpoint", partitionCheckpoint);
                Set mbeans = mbsc.queryNames(null, null);
                //LOG.info("MBean objects: ");
                for(Object mbean : mbeans){
                    ObjectName name = (ObjectName)mbean;
                    //Partition Processed
                    if(isActuallyProcessed(name)){
                        //LOG.info(((ObjectName)mbean).toString());
                        String ok = mbsc.getAttribute(name, "Count").toString();
                        String partitionId = name.getKeyProperty("type");
                        partitionId = partitionId.substring(partitionId.indexOf("Partition") + 10);
                        //LOG.info("Retrieved: " + ok);
                        partitionProcessed.put(partitionId, ok);
                    }else if(isExecutorUtilization(name)){ // Utilization
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        if(Double.parseDouble(ok) >= 0.0) {
                            metrics.put("ExecutorUtilization", Double.parseDouble(ok));
                        }
                    }else if(isExecutorRunning(name)){ //Running
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        metrics.put("ExecutorRunning", Boolean.parseBoolean(ok));
                    }else if(isProcessCpuUsage(name)){ //Check jvm cpu utilization
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        metrics.put("ProcessCpuUsage", Double.parseDouble(ok));
                    }else if(isExecutorServiceRate(name)){
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        metrics.put("ServiceRate", Double.parseDouble(ok));
                    }else{ //Partition WaterMark and CheckpointOffset
                        for(String topic: topics) {
                            if (isWaterMark(name, topic)) { //Watermark
                                //LOG.info(mbean.toString());
                                String ok = mbsc.getAttribute(name, "Value").toString();
                                String partitionId = name.getKeyProperty("name");
                                int i = partitionId.indexOf('-', 6 + topic.length());
                                i++;
                                int j = partitionId.indexOf('-', i);
                                partitionId = partitionId.substring(i, j);
                                //LOG.info("Watermark: " + ok);
                                if(!partitionWatermark.containsKey(topic)){
                                    partitionWatermark.put(topic, new HashMap<String, String>());
                                }
                                partitionWatermark.get(topic).put(partitionId, ok);
                        /*if(partitionNextOffset.containsKey(partitionId)){
                            long arrived = Long.parseLong(ok) - Long.parseLong(partitionNextOffset.get(partitionId));
                            if(arrived < 0) arrived = 0;
                            LOG.info("Partition " + partitionId + " arrived: " + arrived);
                            partitionArrived.put(partitionId, String.valueOf(arrived));
                        }*/
                            }else if(isCheckpoint(name, topic)){    //Checkpoint offset
                                String ok = mbsc.getAttribute(name, "Value").toString();
                                if(!ok.equals("")) {
                                    String partitionId = name.getKeyProperty("name");
                                    int i = partitionId.indexOf('-', 6 + topic.length());
                                    i++;
                                    int j = partitionId.indexOf('-', i);
                                    partitionId = partitionId.substring(i, j);
                                    if (!partitionCheckpoint.containsKey(topic)) {
                                        partitionCheckpoint.put(topic, new HashMap<>());
                                    }
                                    partitionCheckpoint.get(topic).put(partitionId, ok);
                                }
                            }
                        }
                    }

                }
            }catch (Exception e){
                LOG.warn("Exception when retrieving " + containerId + "'s metrics from " + url + " : " + e);
            }finally {
                if(jmxc != null){
                    try{
                        jmxc.close();
                    }catch (Exception e){
                        LOG.warn("Exception when closing jmx connection to " + containerId);
                    }
                }
            }
            return metrics;
        }
    }
    Config config;
    Map<String, String> containerRMI;
    YarnLogRetriever yarnLogRetriever;
    String YarnHomePage, jobName, jobId;
    List<String> topics;
    JMXclient jmxClient;
    Map<String, Object> metrics;
    public JMXMetricsRetriever(Config config){
        this.config = config;
        yarnLogRetriever = new YarnLogRetriever();
        YarnHomePage = config.get("yarn.web.address");
        int nTopic = config.getInt( "topic.number", -1);
        topics = new ArrayList<>();
        if(nTopic == -1) {
            topics.add(config.get("topic.name").toLowerCase());
        }else{
            //Start from 1.
            for(int i=1;i<=nTopic;i++){
                topics.add(config.get("topic."+i+".name").toLowerCase());
            }
        }
        jobName = config.get("job.name");
        jobId = config.get("job.id");
        jmxClient = new JMXclient();
        metrics = new HashMap<>();
        containerRMI = new HashMap<>();
    }

    @Override
    public void init(){
        partitionBeginOffset = new HashMap<>();
        partitionProcessed = new HashMap<>();
        partitionWatermark = new HashMap<>();
        partitionCheckpoint = new HashMap<>();
        partitionArrived = new HashMap<>();
        executorUtilization = new HashMap<>();
        executorServiceRate = new HashMap<>();
        executorRunning = new HashMap<>();
        partitionValid = new HashMap<>();

    }
    /*
        Currently, metrics retriever only support one topic metrics

        Attention:
        1) BeginOffset is set to be the initial highwatermark, so please don't give any input at the beginning.
        2) After migration, containers still contain migrated partitions' metrics (offset)
        3) Return partition ID is in "Partition XXX" format
        4) Even JMX metrics are not correct after migration?
     */

    HashMap<String, Long> partitionProcessed, partitionArrived;
    HashMap<String, Double> executorUtilization, executorServiceRate;
    HashMap<String, Boolean> executorRunning;
    HashMap<String, Boolean> partitionValid;
    HashMap<String, HashMap<String, Long>> partitionWatermark, partitionBeginOffset, partitionCheckpoint;

    //Return a bad flag.
    @Override
    public Map<String, Object> retrieveMetrics(){
        //Debugging
        LOG.info("Start retrieving metrics...");
        Runtime runtime = Runtime.getRuntime();
        NumberFormat format = NumberFormat.getInstance();
        StringBuilder sb = new StringBuilder();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        sb.append("free memory: " + format.format(freeMemory / 1024) + ", ");
        sb.append("allocated memory: " + format.format(allocatedMemory / 1024) + ", ");
        sb.append("max memory: " + format.format(maxMemory / 1024) + ", ");
        sb.append("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
        LOG.info("Memory, " + sb);

        // In metrics, topic will be changed to lowercase

        //Debugging
        LOG.info("Start retrieving AppId");

        String appId = yarnLogRetriever.retrieveAppId(YarnHomePage,jobName + "_" + jobId);

        //Debugging
        LOG.info("Start retrieving Containers' address");

        List<String> containers = yarnLogRetriever.retrieveContainersAddress(YarnHomePage, appId);

        //Debugging
        LOG.info("Start retrieving Containers' JMX url");
        containerRMI.clear();
        yarnLogRetriever.retrieveContainerJMX(containerRMI, containers);
        containers.clear();

        //Debugging
        LOG.info("Start retrieving Checkpoint offsets url");

        //Map<String, HashMap<String, Long>> checkpointOffset = yarnLogRetriever.retrieveCheckpointOffsets(containers, topics);
        metrics.clear();

        LOG.info("Retrieving metrics from JMX...... ");
        partitionArrived.clear();
        executorUtilization.clear();
        executorRunning.clear();
        executorServiceRate.clear();
        partitionValid.clear();
        metrics.put("Arrived", partitionArrived);
        metrics.put("Processed", partitionProcessed);
        metrics.put("Utilization", executorUtilization);
        metrics.put("Running", executorRunning);
        metrics.put("Validity", partitionValid); //For validation check
        metrics.put("ServiceRate", executorServiceRate);
        //HashMap<String, String> debugProcessed = new HashMap<>();
        //HashMap<String, HashMap<String, String>> debugWatermark = new HashMap<>();
        HashMap<String, Set<String>> retrievedWatermarks = new HashMap<>();
        HashMap<String, Double> processCpuUsage = new HashMap<>(), systemCpuUsage = new HashMap<>();
        for(Map.Entry<String, String> entry: containerRMI.entrySet()){
            String containerId = entry.getKey();
            Map<String, Object> ret = jmxClient.retrieveMetrics(containerId, topics, entry.getValue());

            if(ret.containsKey("PartitionCheckpoint")){
                HashMap<String, HashMap<String, String>> checkpoint = (HashMap<String, HashMap<String, String>>)ret.get("PartitionCheckpoint");
                //LOG.info("Debug, checkpoint=" + checkpoint);
                for(String topic: topics){
                    if(checkpoint.containsKey(topic)){
                        if(!partitionCheckpoint.containsKey(topic))partitionCheckpoint.put(topic, new HashMap<>());
                        HashMap<String, Long> checkp = partitionCheckpoint.get(topic);
                        for (Map.Entry<String, String> ent : checkpoint.get(topic).entrySet()) {
                            if(!checkp.containsKey(ent.getKey())){
                                checkp.put(ent.getKey(), Long.parseLong(ent.getValue()));
                            }else{
                                long value = Long.parseLong(ent.getValue());
                                if(value > checkp.get(ent.getKey())){
                                    checkp.put(ent.getKey(), value);
                                }
                            }
                        }
                    }
                }
            }
            /*
                Watermark is in format of:
                <Topic, Map<Partition, Watermark>>
             */
            if(ret.containsKey("PartitionWatermark")) {
                HashMap<String, HashMap<String, String>> watermark = (HashMap<String, HashMap<String, String>>)ret.get("PartitionWatermark");
                for(String topic: topics) {
                    if(watermark.containsKey(topic)) {
                        HashMap<String, Long> beginOffset, pwatermark;
                        if (!partitionBeginOffset.containsKey(topic)){
                            partitionBeginOffset.put(topic, new HashMap<>());
                        }
                        beginOffset = partitionBeginOffset.get(topic);
                        if (!partitionWatermark.containsKey(topic)){
                            partitionWatermark.put(topic, new HashMap<>());
                        }
                        pwatermark = partitionWatermark.get(topic);
                        for (Map.Entry<String, String> ent : watermark.get(topic).entrySet()) {
                            /*if(!debugWatermark.containsKey(topic)){
                                debugWatermark.put(topic, new HashMap<>());
                            }
                            debugWatermark.get(topic).put(containerId + ent.getKey(), ent.getValue());*/
                            //To check whether all partitions' watermark is ready.
                            if(!retrievedWatermarks.containsKey(topic)){
                                retrievedWatermarks.put(topic, new HashSet<>());
                            }
                            retrievedWatermarks.get(topic).add(ent.getKey());

                            if (!beginOffset.containsKey(ent.getKey())) {
                                beginOffset.put(ent.getKey(), Long.parseLong(ent.getValue()));
                            }

                            if (!pwatermark.containsKey(ent.getKey())) {
                                pwatermark.put(ent.getKey(), Long.parseLong(ent.getValue()));
                            } else {
                                long value = Long.parseLong(ent.getValue());
                                if (value >= pwatermark.get(ent.getKey())) {
                                    pwatermark.put(ent.getKey(), value);
                                }
                            }
                        }
                    }
                }
            }
            if(ret.containsKey("PartitionProcessed")) {
                HashMap<String, String> processed = (HashMap<String, String>)ret.get("PartitionProcessed");
                for(Map.Entry<String, String> ent : processed.entrySet()) {
                    String partitionId = "Partition " + ent.getKey();
                    long val = Long.parseLong(ent.getValue());
                    for(String topic:topics){
                        if(partitionCheckpoint.containsKey(topic) && partitionBeginOffset.containsKey(topic) && partitionBeginOffset.get(topic).containsKey(ent.getKey()) && partitionCheckpoint.get(topic).containsKey(ent.getKey())){
                            long t = partitionCheckpoint.get(topic).get(ent.getKey()) - partitionBeginOffset.get(topic).get(ent.getKey()) + 1;
                            if(t > 0) val += t;
                        }
                    }
                    //debugProcessed.put(containerId + partitionId, String.valueOf(val));
                    if (!partitionProcessed.containsKey(partitionId)) {
                        partitionProcessed.put(partitionId, val);
                        partitionValid.put(partitionId, true);
                    } else {
                        if (val >= partitionProcessed.get(partitionId)) {
                            partitionProcessed.put(partitionId, val);
                            partitionValid.put(partitionId, true);
                        }else{
                            LOG.warn(partitionId + "'s processed is still smaller then last: processed=" + partitionProcessed.get(partitionId) + " offset=" + val);
                            partitionValid.put(partitionId, true); //partitionValid.put(partitionId, false);
                        }
                    }
                }
            }
            if(ret.containsKey("ExecutorUtilization")){
                processCpuUsage.put(containerId, (Double)ret.get("ExecutorUtilization"));
                //executorUtilization.put(containerId, (Double)ret.get("ExecutorUtilization"));
            }
            if(ret.containsKey("ExecutorRunning")){
                executorRunning.put(containerId, (Boolean)ret.get("ExecutorRunning"));
            }
            /*if(ret.containsKey("SystemCpuUsage")){
                systemCpuUsage.put(containerId, (Double) ret.get("SystemCpuUsage"));
            }*/
            if(ret.containsKey("ProcessCpuUsage")){
                executorUtilization.put(containerId, ((Double)ret.get("ProcessCpuUsage")) / 3.125);
            }
            if(ret.containsKey("ServiceRate")){
                double t = (Double)ret.get("ServiceRate");
                if(!Double.isNaN(t)){
                    executorServiceRate.put(containerId, ((Double)ret.get("ServiceRate")) * 1e3);
                }

            }
        }
        //Why need this? Translate
        if(partitionWatermark.containsKey(topics.get(0))) {
            for (String partitionId : partitionWatermark.get(topics.get(0)).keySet()) {
                long arrived = 0;
                boolean allExisted = true;
                for (String topic : topics) {
                    long watermark = partitionWatermark.get(topic).get(partitionId);
                    long begin = partitionBeginOffset.get(topic).get(partitionId);
                    if(!retrievedWatermarks.containsKey(topic) || !retrievedWatermarks.get(topic).contains(partitionId)){
                        partitionValid.put("Partition " + partitionId, false);
                    }
                    arrived += watermark - begin;
                }
                long processed = partitionProcessed.getOrDefault("Partition " + partitionId, 0l);
                if (arrived < processed) {
                    LOG.warn("Attention, partition " + partitionId + "'s arrival is smaller than processed, arrival: " + arrived + " processed: " + processed);
                    arrived = processed;
                    if(arrived + 1 < processed)partitionValid.put("Partition " + partitionId, false);
                }
                partitionArrived.put("Partition " + partitionId, arrived);

            }
        }

        /*LOG.info("Debugging, retrieved watermark: " + debugWatermark);
        LOG.info("Debugging, checkpoint: " + partitionCheckpoint);
        LOG.info("Debugging, processed: " + debugProcessed);
        LOG.info("Debugging, begin: " + partitionBeginOffset);
        LOG.info("Debugging, valid: " + partitionValid);*/
        LOG.info("Retrieved Metrics: " + metrics);
        //Check CPU usage
        System.out.println("Process CPU Usage: " + processCpuUsage);
        //System.out.println("System CPU Usage: " + systemCpuUsage);
        //Debugging
        runtime = Runtime.getRuntime();
        sb = new StringBuilder();
        maxMemory = runtime.maxMemory();
        allocatedMemory = runtime.totalMemory();
        freeMemory = runtime.freeMemory();
        sb.append("free memory: " + format.format(freeMemory / 1024) + ", ");
        sb.append("allocated memory: " + format.format(allocatedMemory / 1024) + ", ");
        sb.append("max memory: " + format.format(maxMemory / 1024) + ", ");
        sb.append("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
        LOG.info("Memory, " + sb);

        return metrics;
    }
}
