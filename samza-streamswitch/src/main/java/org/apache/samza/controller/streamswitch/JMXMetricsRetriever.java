package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.net.URL;
import java.net.URLConnection;
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
        protected Map<String, String> retrieveContainerJMX(List<String> containerAddress){
            Map<String, String> containerJMX = new HashMap<>();
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
                                    String caddress = address + "/samza-container-" + content.substring(in + 16, ind) + "-startup.log/?start=-8000";  //Read jmx url from startup.log
                                    Map.Entry<String, String> ret = retrieveContainerJMX(caddress);
                                    if(ret == null){ //Cannot retrieve JMXRMI for some reason
                                        LOG.info("Cannot retrieve container's JMX from : " + caddress + ", report error");
                                    }else {
                                        LOG.info("container's JMX: " + ret);
                                        String host = url.split("[\\:]")[1].substring(2);
                                        String jmxRMI = ret.getValue().replaceAll("localhost", host);
                                        containerJMX.put(ret.getKey(), jmxRMI);
                                    }
                                }
                            }
                        }
                    }
                }catch (Exception e){
                    LOG.info("Exception happened when retrieve containers' JMX address : " + e);
                }
            }
            return containerJMX;
        }
        protected Map.Entry<String, String> retrieveContainerJMX(String address){
            String containerId = null, JMXaddress = null;
            try{
                String url = address;
                //LOG.info("Try to retrieve container's JMXRMI from url: " + url);
                URLConnection connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.contains("[id=")){
                        int i = content.indexOf("[id=")+4;
                        containerId = content.substring(i, i+6);
                    }
                    if(content.contains("url=service:")){
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
                return new DefaultMapEntry(containerId, JMXaddress);
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
        private boolean isNextOffset(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").contains("-offset-change")
                    && !name.getKeyProperty("name").contains("window-count") && !name.getKeyProperty("name").contains(topic + "-changelog-");
        }

        private boolean isActuallyProcessed(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.TaskInstanceMetrics") && name.getKeyProperty("name").equals("messages-actually-processed");
        }

        private boolean isExecutorUtilization(ObjectName name){
            return name.getDomain().equals("org.apache.samza.container.SamzaContainerMetrics") && name.getKeyProperty("name").equals("average-utilization");
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
                HashMap<String, String> partitionArrived = new HashMap<>(), partitionProcessed = new HashMap<>();
                HashMap<String, HashMap<String,String>>partitionWatermark = new HashMap<>();
                //metrics.put("PartitionArrived", partitionArrived);
                metrics.put("PartitionWatermark", partitionWatermark);
                //metrics.put("PartitionNextOffset", partitionNextOffset);
                metrics.put("PartitionProcessed", partitionProcessed);
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
                        if(!metrics.containsKey("PartitionProcessed")){
                            metrics.put("PartitionProcessed", new HashMap<String, String>());
                        }
                        ((HashMap<String, String>) (metrics.get("PartitionProcessed"))).put(partitionId, ok);
                    }
                    else if(isExecutorUtilization(name)){
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        metrics.put("ExecutorUtilization", Double.parseDouble(ok));
                    }else { //Partition WaterMark
                        for(String topic: topics) {
                            if (isWaterMark(name, topic)) {
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
    public JMXMetricsRetriever(Config config){
        this.config = config;
    }

    @Override
    public void init(){
        partitionBeginOffset = new HashMap<>();
        partitionProcessed = new HashMap<>();
        partitionWatermark = new HashMap<>();
    }
    /*
        Currently, metrics retriever only support one topic metrics

        Attention:
        1) BeginOffset is set to be the initial highwatermark, so please don't give any input at the beginning.
        2) After migration, containers still contain migrated partitions' metrics (offset)
        3) Return partition ID is in "Partition XXX" format
        4) Even JMX metrics are not correct after migration?
     */

    HashMap<String, Long> partitionProcessed;
    HashMap<String, HashMap<String, Long>> partitionWatermark, partitionBeginOffset;

    //Return a bad flag.
    @Override
    public Map<String, Object> retrieveMetrics(){
        YarnLogRetriever yarnLogRetriever = new YarnLogRetriever();
        String YarnHomePage = config.get("yarn.web.address");
        int nTopic = config.getInt( "topic.number", -1);
        List<String> topics = new ArrayList<>();
        if(nTopic == -1) {
            topics.add(config.get("topic.name").toLowerCase());
        }else{
            //Start from 1.
            for(int i=1;i<=nTopic;i++){
                topics.add(config.get("topic."+i+".name").toLowerCase());
            }
        }
        String jobName = config.get("job.name");
        String jobId = config.get("job.id");
        // In metrics, topic will be changed to lowercase
        String appId = yarnLogRetriever.retrieveAppId(YarnHomePage,jobName + "_" + jobId);
        List<String> containers = yarnLogRetriever.retrieveContainersAddress(YarnHomePage, appId);
        containerRMI = yarnLogRetriever.retrieveContainerJMX(containers);
        Map<String, HashMap<String, Long>> checkpointOffset = yarnLogRetriever.retrieveCheckpointOffsets(containers, topics);
        Map<String, Object> metrics = new HashMap<>();
        JMXclient jmxClient = new JMXclient();
        LOG.info("Retrieving metrics...... ");
        HashMap<String, Long> partitionArrived = new HashMap<>();
        HashMap<String, Double> executorUtilization = new HashMap<>();
        HashMap<String, Boolean> partitionValid = new HashMap<>();
        metrics.put("Arrived", partitionArrived);
        metrics.put("Processed", partitionProcessed);
        metrics.put("Utilization", executorUtilization);
        metrics.put("Validity", partitionValid); //For validation check
        HashMap<String, String> debugProcessed = new HashMap<>();
        HashMap<String, HashMap<String, String>> debugWatermark = new HashMap<>();
        for(Map.Entry<String, String> entry: containerRMI.entrySet()){
            String containerId = entry.getKey();
            Map<String, Object> ret = jmxClient.retrieveMetrics(containerId, topics, entry.getValue());
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
                            if(!debugWatermark.containsKey(topic)){
                                debugWatermark.put(topic, new HashMap<>());
                            }
                            debugWatermark.get(topic).put(containerId + ent.getKey(), ent.getValue());
                            if (!beginOffset.containsKey(ent.getKey())) {
                                beginOffset.put(ent.getKey(), Long.parseLong(ent.getValue()));
                            }

                            if (!pwatermark.containsKey(ent.getKey())) {
                                pwatermark.put(ent.getKey(), Long.parseLong(ent.getValue()));
                            } else {
                                long value = Long.parseLong(ent.getValue());
                                if (value > pwatermark.get(ent.getKey())) {
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
                        if(checkpointOffset.containsKey(topic) && partitionBeginOffset.containsKey(topic) && checkpointOffset.get(topic).containsKey(ent.getKey())){
                            long t = checkpointOffset.get(topic).get(ent.getKey()) - partitionBeginOffset.get(topic).get(ent.getKey());
                            if(t > 0) val += t;
                        }
                    }
                    debugProcessed.put(containerId + partitionId, String.valueOf(val));
                    if (!partitionProcessed.containsKey(partitionId)) {
                        partitionProcessed.put(partitionId, val);
                        partitionValid.put(partitionId, true);
                    } else {
                        if (val >= partitionProcessed.get(partitionId)) {
                            partitionProcessed.put(partitionId, val);
                            partitionValid.put(partitionId, true);
                        }else partitionValid.put(partitionId, false);
                    }
                }
            }
            if(ret.containsKey("ExecutorUtilization")){
                executorUtilization.put(containerId, (Double)ret.get("ExecutorUtilization"));
            }
        }
        //Why need this? Translate
        if(partitionWatermark.containsKey(topics.get(0))) {
            for (String partitionId : partitionWatermark.get(topics.get(0)).keySet()) {
                long arrived = 0;
                for (String topic : topics) {
                    long watermark = partitionWatermark.get(topic).get(partitionId);
                    long begin = partitionBeginOffset.get(topic).get(partitionId);
                    arrived += watermark - begin;
                }
                long processed = partitionProcessed.getOrDefault("Partition " + partitionId, 0l);
                if (arrived < processed) {
                    LOG.info("Attention, partition " + partitionId + "'s arrival is smaller than processed, arrival: " + arrived + " processed: " + processed);
                    arrived = processed;
                    partitionValid.put("Partition " + partitionId, false);
                }
                partitionArrived.put("Partition " + partitionId, arrived);
            }
        }
        /*LOG.info("Debugging, watermark: " + debugWatermark);
        LOG.info("Debugging, checkpoint: " + checkpointOffset);
        LOG.info("Debugging, processed: " + debugProcessed);
        LOG.info("Debugging, begin: " + partitionBeginOffset);
        LOG.info("Debugging, valid: " + partitionValid);*/
        LOG.info("Retrieved Metrics: " + metrics);
        return metrics;
    }

    //For testing
    public static void main(String args[]){
        YarnLogRetriever yarnLogRetriever = new YarnLogRetriever();
        System.out.println("Testing YarnLogRetriever from: " + args[0]);
        String appId = yarnLogRetriever.retrieveAppId(args[0], args[0]);
        System.out.println("Retrieved appId is " + appId);
        List<String> containerAddress = yarnLogRetriever.retrieveContainersAddress(args[0], appId);
        System.out.println("Retrieved containers' address : " + containerAddress);
        Map<String, String> containerRMI = yarnLogRetriever.retrieveContainerJMX(containerAddress);
        System.out.println("Retrieved containers' RMI url : " + containerRMI);
        JMXclient jmxClient = new JMXclient();

        System.out.println("Topic name: " + args[1]);
        String topic = args[1];

        Map<String, Object> metrics = new HashMap<>();
        System.out.println("Retrieving metrics: ");
        metrics.put("PartitionArrived", new HashMap());
        metrics.put("PartitionProcessed", new HashMap());
        metrics.put("ProcessCPUTime", new HashMap());
        metrics.put("Time", new HashMap<>());
        for(Map.Entry<String, String> entry: containerRMI.entrySet()){
            String containerId = entry.getKey();
            Map<String, Object> ret = new HashMap<>();//jmxClient.retrieveMetrics(containerId, topic, entry.getValue());
            if(ret.containsKey("PartitionWatermark")) {
                ((HashMap<String, Object>)metrics.get("PartitionWatermark")).put(containerId, ret.get("PartitionWatermark"));
            }
            if(ret.containsKey("PartitionNextOffset")) {
                ((HashMap<String, Object>)metrics.get("PartitionNextOffset")).put(containerId, ret.get("PartitionNextOffset"));
            }
            if(ret.containsKey("PartitionProcessed")) {
                ((HashMap<String, Object>)metrics.get("PartitionProcessed")).put(containerId, ret.get("PartitionProcessed"));
            }
            if(ret.containsKey("ProcessCPUTime")){
                ((HashMap<String, Object>)metrics.get("ProcessCPUTime")).put(containerId, ret.get("ProcessCPUTime"));
            }
            if(ret.containsKey("Time")){
                ((HashMap<String, Object>)metrics.get("Time")).put(containerId, ret.get("Time"));
            }
        }
        System.out.println("Retrieved Metrics: " + metrics);

        return ;
    }
}
