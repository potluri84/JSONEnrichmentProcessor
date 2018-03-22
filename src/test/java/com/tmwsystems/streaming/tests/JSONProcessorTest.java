package com.tmwsystems.streaming.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.sam.enrichment.processor.JSONEnrichmentProcessor;


public class JSONProcessorTest {
	
	protected static final Logger LOG = LoggerFactory.getLogger(JSONProcessorTest.class);
	private static final Object TEST_CONFIG_JSON_SCHEMA = "/";
	
	
	@Test
	public void testHREnrichmentNonSecureCluster() throws Exception {
		JSONEnrichmentProcessor enrichProcessor = new JSONEnrichmentProcessor();
		Map<String, Object> processorConfig = createHREnrichmentConfig();
		enrichProcessor.validateConfig(processorConfig);
		enrichProcessor.initialize(processorConfig);
		
		List<StreamlineEvent> eventResults = enrichProcessor.process(createStreamLineEvent());

		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResults));
		

	}
	
	
	private StreamlineEvent createStreamLineEvent() {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("lat", 12343);
		keyValues.put("lon", 12343);keyValues.put("ckdt", 12343);keyValues.put("driverid", 12343);
		keyValues.put("truckid", 12343);keyValues.put("rowkey", 12343);keyValues.put("bol", 12343);
		
		
		StreamlineEvent event = StreamlineEventImpl.builder().build().addFieldsAndValues(keyValues);
		
		System.out.println("Input StreamLIne event is: " + ReflectionToStringBuilder.toString(event));

		
		return event;
	}

	private Map<String, Object> createHREnrichmentConfig() {
		Map<String, Object> processorConfig = new HashMap<String, Object>();
		processorConfig.put(JSONEnrichmentProcessor.CONFIG_JSON_SCHEMA,TEST_CONFIG_JSON_SCHEMA);
		return processorConfig;
	}

}
