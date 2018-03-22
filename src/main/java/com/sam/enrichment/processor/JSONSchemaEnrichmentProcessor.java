package com.sam.enrichment.processor;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;


public class JSONSchemaEnrichmentProcessor implements CustomProcessorRuntime {
	protected static final Logger LOG = LoggerFactory.getLogger(JSONSchemaEnrichmentProcessor.class);
	public static final String CONFIG_JSON_SCHEMA = "jsonSchema";
	public static final String CONFIG_CONSTANT_FIELDS = "constantFields";

	Map<String, Object> config;
	private String jsonSchema;
	private String defaultValues;
	Map<String,Object> defaultsWithValues;
	
	
	public JSONSchemaEnrichmentProcessor()
	{
		
	}
	
	
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + JSONSchemaEnrichmentProcessor.class.getName());

		this.jsonSchema = ((String) config.get(CONFIG_JSON_SCHEMA)).trim();
		this.defaultValues = ((String) config.get(CONFIG_CONSTANT_FIELDS)).trim();
		
		String[] defaultList  = this.defaultValues.split(";");
		defaultsWithValues = new HashMap<String, Object>();
		for (String defaults : defaultList) {
			String[] keyvalues = defaults.split("=");
			defaultsWithValues.put(keyvalues[0], keyvalues[1]);
		}
		
		
		
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
			Gson gson = new Gson();
			JsonObject jsonObject = new JsonObject();
			String[] keyList = this.jsonSchema.split(",");
			Set<String> keys = this.defaultsWithValues.keySet();
			for (String key : keyList) {
				if(keys.contains(key))
				{
					jsonObject.addProperty(key, this.defaultsWithValues.get(key).toString());
				}
				else
				{
					jsonObject.addProperty(key, streamlineEvent.get(key).toString());
				}
			}

			String json = gson.toJson(jsonObject);
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
