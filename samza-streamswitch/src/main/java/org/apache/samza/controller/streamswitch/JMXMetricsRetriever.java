package org.apache.samza.controller.streamswitch;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    class YarnLogRetriever{
        String YARNHomePage;
        String appId;
        protected YarnLogRetriever(){
            YARNHomePage = config.get("yarn.web.address");
        }
        protected String retrieveAppId(){
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
                    String content = scanner.next();
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
        protected List<String> retrieveContainersAddress(){
            List<String> containerAddress = new LinkedList<>();
            String url = "http://" + YARNHomePage + "/cluster/appsattempt/appattempt_" + appId + "_000001";
            try{
                LOG.info("Try to retrieve containers' address from : " + url);
                URLConnection connection = new URL(url).openConnection();
                Scanner scanner = new Scanner(connection.getInputStream());
                scanner.useDelimiter("\n");
                while(scanner.hasNext()){
                    String content = scanner.next();
                    String [] contents = content.split(",");
                    if(contents.length == 4 && contents[0].startsWith("\"<a href='")){
                        String address = contents[3].split("'")[1];
                        containerAddress.add(address);
                    }
                }
            }catch (Exception e){
                LOG.info("Exception happened when retrieve containers address");
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
                        String content = scanner.next();
                        if(content.startsWith("            <a href=\"/node/containerlogs/container")){
                            
                        }
                    }
                }catch (Exception e){

                }
            }
        }
    }
    Config config;
    public JMXMetricsRetriever(Config config){
        this.config = config;
    }
    @Override
    public void init(){

    }
    @Override
    public Map<String, Object> retrieveMetrics(){
        return null;
    }
}
