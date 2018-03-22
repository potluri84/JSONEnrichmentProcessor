package com.sam.enrichment.processor;

import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;


public class JSONEnrichmentProcessor implements CustomProcessorRuntime {
	protected static final Logger LOG = LoggerFactory.getLogger(JSONEnrichmentProcessor.class);
	public static final String CONFIG_JSON_SCHEMA = "jsonSchema";

	Map<String, Object> config;
	private String jsonSchema;

	public JSONEnrichmentProcessor()
	{
		
	}
	
	
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + JSONEnrichmentProcessor.class.getName());

		this.jsonSchema = ((String) config.get(CONFIG_JSON_SCHEMA)).trim();
		LOG.info("The configured JSON Schema is: " + jsonSchema);
	}

	public void validateConfig(Map<String, Object> config) throws ConfigException {
	}

	public List<StreamlineEvent> process(StreamlineEvent streamlineEvent) throws ProcessingException {
		LOG.info("Event[" + streamlineEvent + "] about to be enriched");
		StreamlineEvent enrichedEvent = null;
		Map<String, Object> enrichValues = null;

		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		builder.putAll(streamlineEvent);

		try {
			enrichValues = convertToJsonSchema(streamlineEvent);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ProcessingException(e.getMessage(),e.getCause());
		}
		LOG.info("Enriching events[" + streamlineEvent + "]  with the following enriched values: " + enrichValues);
		builder.putAll(enrichValues);

		enrichedEvent = builder.dataSourceId(streamlineEvent.getDataSourceId()).build();
		LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

		List<StreamlineEvent> newEvents = Collections.<StreamlineEvent>singletonList(enrichedEvent);

		return newEvents;

	}

	private Map<String, Object> convertToJsonSchema(StreamlineEvent streamlineEvent) throws Exception {
		Map<String, Object> enrichedValues = new HashMap<String, Object>();
		try
		{
			BlockchainData blockchainData = new BlockchainData(); // replace all this with dynamic json schema
			blockchainData.setBol(Long.parseLong(streamlineEvent.get("bol").toString()));
            blockchainData.setId(streamlineEvent.get("rowkey").toString());
            blockchainData.setDriverId(streamlineEvent.get("driverid").toString());
            blockchainData.setTruckId(streamlineEvent.get("truckid").toString());
            blockchainData.setData("{" + blockchainData.buildLocationJson(streamlineEvent.get("lat"), streamlineEvent.get("lon"), streamlineEvent.get("ckdt")) + ", " + blockchainData.getAddressJson() + "}");
            
            Gson gson = new Gson();
            Type type = new TypeToken<BlockchainData>() {}.getType();
            String json = gson.toJson(blockchainData, type);
            
			enrichedValues.put("payload",json);
		}
		catch(Exception ex)
		{
			LOG.error(ex.getMessage());
			throw ex;
		}
		return enrichedValues;
	}

	public void cleanup() {
		LOG.debug("Cleaning up");
	}
}
