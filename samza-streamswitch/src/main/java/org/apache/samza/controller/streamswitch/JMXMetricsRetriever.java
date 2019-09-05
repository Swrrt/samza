package org.apache.samza.controller.streamswitch;

import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
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
                LOG.info("Exception happened when retrieve AppId : " + e.getCause());
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
                                    //LOG.info("container's JMX: " + ret);
                                    String host = url.split("[\\:]")[1].substring(2);
                                    String jmxRMI = ret.getValue().replaceAll("localhost", host);
                                    containerJMX.put(ret.getKey(), jmxRMI);
                                }
                            }
                        }
                    }
                }catch (Exception e){
                    LOG.info("Exception happened when retrieve containers JMX address : " + e);
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
    static class JMXclient{
        protected Map<String, String> retrieveMetrics(String url){
            Map<String, String> metrics = new HashMap<>();
            LOG.info("Try to retrieve metrics from " + url);
            try{
                JMXServiceURL jmxServiceURL = new JMXServiceURL(url);
                LOG.info("Connecting JMX server...");
                JMXConnector jmxc = JMXConnectorFactory.connect(jmxServiceURL, null);
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                Set mbeans = mbsc.queryNames(null, null);
                LOG.info("MBean objects: ");
                for(Object mbean : mbeans){
                    ObjectName name = (ObjectName)mbean;
                    if(name.getDomain().equals("org.apache.samza.container.TaskInstanceMetrics") && name.getCanonicalName().equals("messages-actually-processed")){
                        LOG.info(((ObjectName)mbean).toString());
                        ObjectInstance instance = mbsc.getObjectInstance(name);
                        String ok = mbsc.getAttribute(name, null).toString();
                        LOG.info("Retrieved: " + ok);
                    }
                }
                //mbsc.getObjectInstance()
                /*ObjectName name = new ObjectName("org.apache.samza.container.TaskInstanceMetrics");
                ObjectInstance instance = mbsc.getObjectInstance(name);
                LOG.info("Task instance metrics: " + instance);
                metrics.put("task", instance.toString());*/
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
        YarnLogRetriever yarnLogRetriever = new YarnLogRetriever();
        String YarnHomePage = config.get("yarn.web.address");
        String appId =yarnLogRetriever.retrieveAppId(YarnHomePage);
        List<String> containers = yarnLogRetriever.retrieveContainersAddress(YarnHomePage, appId);
        containerRMI = yarnLogRetriever.retrieveContainerJMX(containers);
    }
    @Override
    public Map<String, Object> retrieveMetrics(){
        Map<String, Object> metrics = new HashMap<>();

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
        //TODO: connect to JMX
        JMXclient jmXclient = new JMXclient();
        for(Map.Entry<String, String> entry: containerRMI.entrySet()){
            String containerId = entry.getKey();
            Map<String, String> metrics = jmXclient.retrieveMetrics(entry.getValue());
            System.out.println("Container " + containerId + " metrics: " + metrics);
        }

        return ;
    }
}
