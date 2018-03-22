package com.sam.enrichment.processor;

import java.sql.Timestamp;

public class BlockchainData {

	 private String id;
     private String data;
     private String driverId;
     private Long bol;
     private String truckId;
     
    /* public String buildLocationJson(Double lat, Double lon, Timestamp time) {                 
         return "\"location\":{\"lat\":\"" + lat.toString() +"\",\"lon\":\"" + lon.toString() + "\",\"gpstimestamp\":\"" + time.toString() + "\"}";
     }*/
     
     public String getAddressJson() {
         return "\"address\":{\"street\":\"abcd\",\"city\":\"city-1\",\"state\":\"OH\",\"zip\":\"abc-123\"";
     }
     
     public String getId() {
         return id;
     }
     public void setId(String ID) {
         this.id = ID;
     }
     public String getData() {
         return data;
     }
     public void setData(String data) {
         this.data = data;
     }
     public String getTruckId() {
         return truckId;
     }
     public void setTruckId(String truckId) {
         this.truckId = truckId;
     }
     
     public String getDriverId() {
         return driverId;
     }
     
     public void setDriverId(String driverId) {
         this.driverId = driverId;
     }
     
     public Long getBol() {
         return bol;
     }
     
     public void setBol(Long bol) {
         this.bol = bol;
     }

	public String buildLocationJson(Object lat, Object lon, Object time) {
		return "\"location\":{\"lat\":\"" + lat.toString() +"\",\"lon\":\"" + lon.toString() + "\",\"gpstimestamp\":\"" + time.toString() + "\"}";
	}

	
}
