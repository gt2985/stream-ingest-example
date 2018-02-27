package com.gcp.apps.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocEvent implements Serializable, Cloneable{


	/**
	 * 
	 */
	private static final long serialVersionUID = -4220002443862088578L;
	private String jobId;
	private String schedDate;
	private String distnTime;
	private String distnClient;
	private String clientId;
	private String contactName;
	private String quantity;
	private String contractId;
	private String latitude;
	private String longitude;
	private Float elevation = Float.parseFloat("0.0");
	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}
	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	/**
	 * @return the distnDate
	 */
	public String getSchedDate() {
		return schedDate;
	}
	/**
	 * @param distnDate the distnDate to set
	 */
	public void setSchedDate(String distnDate) {
		this.schedDate = distnDate;
	}
	/**
	 * @return the distnClient
	 */
	public String getDistnClient() {
		return distnClient;
	}
	/**
	 * @param distnClient the distnClient to set
	 */
	public void setDistnClient(String distnClient) {
		this.distnClient = distnClient;
	}
	/**
	 * @return the clientId
	 */
	public String getClientId() {
		return clientId;
	}
	/**
	 * @param clientId the clientId to set
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	/**
	 * @return the contactName
	 */
	public String getContactName() {
		return contactName;
	}
	/**
	 * @param contactName the contactName to set
	 */
	public void setContactName(String contactName) {
		this.contactName = contactName;
	}
	/**
	 * @return the quantity
	 */
	public String getQuantity() {
		return quantity;
	}
	/**
	 * @param quantity the quantity to set
	 */
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}
	/**
	 * @return the contractId
	 */
	public String getContractId() {
		return contractId;
	}
	/**
	 * @param contractId the contractId to set
	 */
	public void setContractId(String contractId) {
		this.contractId = contractId;
	}
	/**
	 * @return the latitude
	 */
	public String getLatitude() {
		return latitude;
	}
	/**
	 * @param latitude the latitude to set
	 */
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	/**
	 * @return the longitude
	 */
	public String getLongitude() {
		return longitude;
	}
	/**
	 * @param longitude the longitude to set
	 */
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	/**
	 * Parses the raw game event info into GameActionInfo objects. Each event line has the following
	 * format: username,teamname,score,timestamp_in_ms,readable_time
	 * e.g.:
	 * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
	 * The human-readable time string is not used here.
	 */
	public static class ParseLocationEventFn extends DoFn<String, LocEvent> {

		// Log and count parse errors.
		private static final Logger LOG = LoggerFactory.getLogger(ParseLocationEventFn.class);
		private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");
		

		@ProcessElement
		public void processElement(ProcessContext c) {
			String payload = c.element();
			try {
				if(payload != null && payload.length() > 0 && payload.contains(",")) {
					String[] objs = payload.split(",");
					LocEvent locEvent = new LocEvent();
					locEvent.setJobId(objs[0].replaceAll("\"", ""));
					DateTime distnDate = DateTime.parse(objs[1].replaceAll("\"", ""), DateTimeFormat.forPattern("yyyyMMdd"));
					locEvent.setSchedDate(distnDate.toString("yyyy-MM-dd"));
					locEvent.setDistnTime(objs[2].replaceAll("\"", ""));
					locEvent.setClientId(objs[3].replaceAll("\"", ""));
					locEvent.setContactName(objs[4].replaceAll("\"", ""));
					locEvent.setQuantity(objs[9].replaceAll("\"", ""));
					locEvent.setContractId(objs[10].replaceAll("\"", ""));
					locEvent.setLatitude(objs[11].replaceAll("\"", ""));
					locEvent.setLongitude(objs[12].replaceAll("\"", ""));
					//DateTime localTime = DateTime.parse(objs[21].replaceAll("\"", ""), DateTimeFormat.forPattern("yyyy-MM-DD-HH.mm.ss.SSSSSS"));
					//locEvent.setLocalTime(ISODateTimeFormat.dateTime().print(localTime.getMillis()));
					LOG.info("converted obj::" + locEvent);
					c.output(locEvent);
				} else {
					LOG.error(" invalid payload found : " + payload);
				}
			} catch (Exception e) {
				e.printStackTrace();
				numParseErrors.inc();
				LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
			}
		}
	}
	
	/**
	 * @return the distnTime
	 */
	public String getDistnTime() {
		return distnTime;
	}
	/**
	 * @param distnTime the distnTime to set
	 */
	public void setDistnTime(String distnTime) {
		this.distnTime = distnTime;
	}
	/**
	 * @return the elevation
	 */
	public Float getElevation() {
		return elevation;
	}
	/**
	 * @param elevation the elevation to set
	 */
	public void setElevation(float elevation) {
		this.elevation = elevation;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

}
