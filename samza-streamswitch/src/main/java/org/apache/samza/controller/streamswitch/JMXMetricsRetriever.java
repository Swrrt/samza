package org.apache.samza.controller.streamswitch;

import javafx.util.Pair;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
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
        protected String retrieveAppId(String YARNHomePage){
            String newestAppId = null;
            String appPrefix = "[\"<a href='/cluster/app/application_";
            URLConnection connection = null;
            try {
                String url = "http://" + YARNHomePage + "/cluster/apps";
                LOG.info("Try to retrieve AppId from : " +  url);
                connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.startsWith(appPrefix)){
                        content = content.substring(appPrefix.length(), appPrefix.length() + 18);
                        if(newestAppId == null || content.compareTo(newestAppId) > 0){
                            newestAppId = content;
                        }
                    }
                }
                LOG.info("Retrieved newest AppId is : " + newestAppId);
            }catch (Exception e){
                LOG.info("Exception happened when retrieve AppIds, exception : " + e);
            }
            return newestAppId;
        }
        protected List<String> retrieveContainersAddress(String YARNHomePage, String appId){
            List<String> containerAddress = new LinkedList<>();
            String url = "http://" + YARNHomePage + "/cluster/appattempt/appattempt_" + appId + "_000001";
            try{
                LOG.info("Try to retrieve containers' address from : " + url);
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
                    LOG.info("Try to retrieve container's log from url: " + url);
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
                                    String caddress = address +"/" + content.substring(in, ind) + ".log/?start=0";
                                    Map.Entry<String, String> ret = retrieveContainerJMX(caddress);
                                    if(ret == null){ //Cannot retrieve JMXRMI for some reason
                                        LOG.info("Cannot retrieve container's JMX, report error");
                                    }else {
                                        //LOG.info("container's JMX: " + ret);
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
                LOG.info("Try to retrieve container's log from url: " + url);
                URLConnection connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next().trim();
                    if(content.contains("pid=")){
                        int i = content.indexOf("pid=")+4;
                        containerId = content.substring(i, i+6);
                    }
                    if(content.contains("Started JmxServer")){
                        int i = content.indexOf("url=") + 4;
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
    }
   private static class JMXclient{
        JMXclient(){
        }
        private boolean isWaterMark(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").contains("-high-watermark") && !name.getKeyProperty("name").contains("-messages-behind-high-watermark")
                    && !name.getKeyProperty("name").contains("window-count");
        }
        private boolean isNextOffset(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.system.kafka.KafkaSystemConsumerMetrics") && name.getKeyProperty("name").startsWith("kafka-" + topic + "-") && name.getKeyProperty("name").contains("-offset-change")
                    && !name.getKeyProperty("name").contains("window-count");
        }

        private boolean isActuallyProcessed(ObjectName name, String topic){
            return name.getDomain().equals("org.apache.samza.container.TaskInstanceMetrics") && name.getKeyProperty("name").equals("messages-actually-processed");
        }
        protected Map<String, Object> retrieveMetrics(String topic, String url){
            Map<String, Object> metrics = new HashMap<>();
            //LOG.info("Try to retrieve metrics from " + url);
            try{
                JMXServiceURL jmxServiceURL = new JMXServiceURL(url);
                //LOG.info("Connecting JMX server...");
                JMXConnector jmxc = JMXConnectorFactory.connect(jmxServiceURL, null);
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
                HashMap<String, String> partitionArrived = new HashMap<>(), partitionWatermark = new HashMap<>(), partitionNextOffset = new HashMap<>(), partitionProcessed = new HashMap<>();
                //metrics.put("PartitionArrived", partitionArrived);
                metrics.put("PartitionWatermark", partitionWatermark);
                metrics.put("PartitionNextOffset", partitionNextOffset);
                metrics.put("PartitionProcessed", partitionProcessed);
                Set mbeans = mbsc.queryNames(null, null);
                //LOG.info("MBean objects: ");
                for(Object mbean : mbeans){
                    ObjectName name = (ObjectName)mbean;
                    //Partition WaterMark
                    if(isWaterMark(name, topic)){
                        //LOG.info(mbean.toString());
                        String ok = mbsc.getAttribute(name, "Value").toString();
                        String partitionId = name.getKeyProperty("name");
                        int i = partitionId.indexOf('-', 6);
                        i++;
                        int j = partitionId.indexOf('-', i);
                        partitionId = partitionId.substring(i, j);
                        //LOG.info("Watermark: " + ok);
                        partitionWatermark.put(partitionId, ok);
                        /*if(partitionNextOffset.containsKey(partitionId)){
                            long arrived = Long.parseLong(ok) - Long.parseLong(partitionNextOffset.get(partitionId));
                            if(arrived < 0) arrived = 0;
                            LOG.info("Partition " + partitionId + " arrived: " + arrived);
                            partitionArrived.put(partitionId, String.valueOf(arrived));
                        }*/
                    }else
                    //Partition next offset
                    if(isNextOffset(name, topic)){
                        //LOG.info(mbean.toString());
                        String ok = mbsc.getAttribute(name, "Count").toString();
                        String partitionId = name.getKeyProperty("name");
                        int i = partitionId.indexOf('-', 6);
                        i++;
                        int j = partitionId.indexOf('-', i);
                        partitionId = partitionId.substring(i, j);
                        //LOG.info("Next offset: " + ok);
                        partitionNextOffset.put(partitionId, ok);
                        /*if(partitionWatermark.containsKey(partitionId)){
                            long arrived =  Long.parseLong(partitionWatermark.get(partitionId))- Long.parseLong(ok);
                            if(arrived < 0) arrived = 0;
                            LOG.info("Partition " + partitionId + " arrived: " + arrived);
                            partitionArrived.put(partitionId, String.valueOf(arrived));
                        }*/
                    }else
                    //Partition Processed
                    if(isActuallyProcessed(name, topic)){
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
                }
            }catch (Exception e){
                LOG.info("Exception when retrieve metrics from " + url + " : " + e);
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
     */

    HashMap<String, Long> partitionWatermark, partitionBeginOffset, partitionProcessed;
    @Override
    public Map<String, Object> retrieveMetrics(){
        YarnLogRetriever yarnLogRetriever = new YarnLogRetriever();
        String YarnHomePage = config.get("yarn.web.address");
        String topic = config.get("topic.name");
        String appId = yarnLogRetriever.retrieveAppId(YarnHomePage);
        List<String> containers = yarnLogRetriever.retrieveContainersAddress(YarnHomePage, appId);
        containerRMI = yarnLogRetriever.retrieveContainerJMX(containers);
        Map<String, Object> metrics = new HashMap<>();
        JMXclient jmxClient = new JMXclient();
        LOG.info("Retrieving metrics: ");
        HashMap<String, Long> partitionArrived = new HashMap<>();
        metrics.put("PartitionArrived", partitionArrived);
        metrics.put("PartitionProcessed", partitionProcessed);
        metrics.put("ProcessCPUTime", new HashMap());
        metrics.put("Time", new HashMap());
        for(Map.Entry<String, String> entry: containerRMI.entrySet()){
            String containerId = entry.getKey();
            Map<String, Object> ret = jmxClient.retrieveMetrics(topic, entry.getValue());
            if(ret.containsKey("PartitionWatermark")) {
                HashMap<String, String> watermark = (HashMap<String, String>)ret.get("PartitionWatermark");
                for(Map.Entry<String, String> ent : watermark.entrySet()){
                    if(!partitionBeginOffset.containsKey(ent.getKey())){
                        partitionBeginOffset.put(ent.getKey(), Long.parseLong(ent.getValue()));
                    }
                    if(!partitionWatermark.containsKey(ent.getKey())){
                        partitionWatermark.put(ent.getKey(), Long.parseLong(ent.getValue()));
                    }else{
                        long value = Long.parseLong(ent.getValue());
                        if(value > partitionWatermark.get(ent.getKey())){
                            partitionWatermark.put(ent.getKey(), value);
                        }
                    }
                }
            }
            if(ret.containsKey("PartitionProcessed")) {
                HashMap<String, String> processed = (HashMap<String, String>)ret.get("PartitionProcessed");
                for(Map.Entry<String, String> ent : processed.entrySet()) {
                    if (!partitionProcessed.containsKey(ent.getKey())) {
                        partitionProcessed.put(ent.getKey(), Long.parseLong(ent.getValue()));
                    } else {
                        long value = Long.parseLong(ent.getValue());
                        if (value > partitionProcessed.get(ent.getKey())) {
                            partitionProcessed.put(ent.getKey(), value);
                        }
                    }
                }
            }
            if(ret.containsKey("ProcessCPUTime")){
                ((HashMap<String, Object>)metrics.get("ProcessCPUTime")).put(containerId, ret.get("ProcessCPUTime"));
            }
            if(ret.containsKey("Time")){
                ((HashMap<String, Object>)metrics.get("Time")).put(containerId, ret.get("Time"));
            }
        }
        for(String partitionId : partitionWatermark.keySet()){
            long begin = partitionBeginOffset.get(partitionId);
            long watermark = partitionWatermark.get(partitionId);
            long processed = partitionProcessed.getOrDefault(partitionId, 0l);
            if(watermark - processed < begin){
                begin = watermark - processed;
                partitionBeginOffset.put(partitionId, begin);
            }
            partitionArrived.put(partitionId, watermark - begin);
        }
        LOG.info("Retrieved Metrics: " + metrics);
        return metrics;
    }

    //For testing
    public static void main(String args[]){
        YarnLogRetriever yarnLogRetriever = new YarnLogRetriever();
        System.out.println("Testing YarnLogRetriever from: " + args[0]);
        String appId = yarnLogRetriever.retrieveAppId(args[0]);
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
            Map<String, Object> ret = jmxClient.retrieveMetrics(topic, entry.getValue());
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
