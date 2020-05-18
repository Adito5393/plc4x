/*
Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
package org.apache.plc4x.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.exceptions.PlcException;
import org.apache.plc4x.java.api.exceptions.PlcIncompatibleDatatypeException;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionResponse;
import org.apache.plc4x.java.scraper.ScrapeJob;
import org.apache.plc4x.java.scraper.config.JobConfigurationImpl;
import org.apache.plc4x.java.scraper.config.ScraperConfiguration;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImpl;
import org.apache.plc4x.java.scraper.exception.ScraperException;
import org.apache.plc4x.java.scraper.triggeredscraper.TriggeredScrapeJobImpl;
import org.apache.plc4x.java.scraper.triggeredscraper.TriggeredScraperImpl;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollector;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class Plc4XConsumer extends DefaultConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Plc4XConsumer.class);

    private ExceptionHandler exceptionHandler;
    private PlcConnection plcConnection;
    private  Map<String,Object> tags;
    private String trigger;
    private PlcSubscriptionResponse subscriptionResponse;
    private Plc4XEndpoint plc4XEndpoint;

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> future;



    public Plc4XConsumer(Plc4XEndpoint endpoint, Processor processor) throws PlcException {
        super(endpoint, processor);
        plc4XEndpoint =endpoint;
        this.plcConnection = endpoint.getConnection();
        this.tags = endpoint.getTags();
        this.trigger= endpoint.getTrigger();
        plc4XEndpoint=endpoint;
    }

    @Override
    public String toString() {
        return "Plc4XConsumer[" + plc4XEndpoint + "]";
    }

    @Override
    public Endpoint getEndpoint() {
        return plc4XEndpoint;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    protected void doStart() throws InterruptedException, ExecutionException {
        if(trigger==null) {
            PlcReadRequest.Builder builder = plcConnection.readRequestBuilder();
            for( String tag : tags.keySet()){
                try{
                    String query = (String)tags.get(tag);
                    builder.addItem(tag,query);
                }
                catch (PlcIncompatibleDatatypeException e){
                    LOGGER.error("For consumer, please use Map<String,String>, currently using {}",tags.getClass().getSimpleName());
                }
            }
            PlcReadRequest request = builder.build();
            future = executorService.schedule(() -> {
                request.execute().thenAccept(response -> {
                    try {
                        Exchange exchange = plc4XEndpoint.createExchange();
                        Map<String,Object> rsp = new HashMap<>();
                        for(String field : response.getFieldNames()){
                            rsp.put(field,response.getObject(field));
                        }
                        exchange.getIn().setBody(rsp);
                        getProcessor().process(exchange);
                    } catch (Exception e) {
                        exceptionHandler.handleException(e);
                    }
                });
            }, 500, TimeUnit.MILLISECONDS);
        }
        else{
            try {
                ScraperConfiguration configuration =  getScraperConfig(validateTags());
                TriggerCollector collector = new TriggerCollectorImpl(plc4XEndpoint.getPlcDriverManager());

                TriggeredScraperImpl scraper = new TriggeredScraperImpl(configuration, (job, alias, response) -> {
                    try {
                        Exchange exchange = plc4XEndpoint.createExchange();
                        exchange.getIn().setBody(response);
                        getProcessor().process(exchange);
                    } catch (Exception e) {
                        exceptionHandler.handleException(e);
                    };
                    },collector);
                scraper.start();
                collector.start();
            } catch (ScraperException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, String> validateTags() {
        Map<String, String> map = new HashMap<>();
        for(Map.Entry<String,Object>tag: tags.entrySet()){
            if(tag.getValue() instanceof String){
                map.put(tag.getKey(),(String)tag.getValue());
            }
        }
        if(map.size()!=tags.size()){
            LOGGER.error("At least one entry does not match the format : Map.Entry<String,String> ");
            return null;
        }
        else return map;
    }

    private ScraperConfigurationTriggeredImpl getScraperConfig(Map<String,String> tagList){
        String config = "(TRIGGER_VAR,"+plc4XEndpoint.getPeriod()+",("+ plc4XEndpoint.getTrigger() +")==(true))";
        List<JobConfigurationImpl> job = Collections.singletonList(new JobConfigurationImpl("PLC4X-Camel",config,0,Collections.singletonList(Constants.PLC_NAME),tagList));
        Map<String,String> source = Collections.singletonMap(Constants.PLC_NAME,plc4XEndpoint.getUri());
        return new ScraperConfigurationTriggeredImpl(source,job);
    }

    @Override
    protected void doStop() throws InterruptedException, ExecutionException, TimeoutException {
        // First stop the polling process
        if (future != null) {
            future.cancel(true);
        }
    }

    private Object unwrapIfSingle(Collection collection) {
        if (collection.isEmpty()) {
            return null;
        }
        if (collection.size() == 1) {
            return collection.iterator().next();
        }
        return collection;
    }

}