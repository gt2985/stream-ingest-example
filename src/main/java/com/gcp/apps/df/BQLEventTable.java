package com.gcp.apps.df;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;

public class BQLEventTable {
	
	public enum FIELD {
		job_id,
		DISTN_DATE,
		CLIENT_ID,
		CONTACT_NAME,
		QUANTITY,
		CONTRACT_ID,
		LATITUDE,
		LONGITUDE,
		ELEVATION;
	}
	
	private TableDestination destination;
	
	private TableReference reference;
	
	private TimePartitioning partitioning;
	
	public String getTableId() {
		return reference.getTableId();
	}
	
	
	public TimePartitioning getPartitioning() {
		return partitioning;
	}

	public void setPartitioning(TimePartitioning partitioning) {
		this.partitioning = partitioning;
	}

	public TableDestination getDestination() {
		return destination;
	}

	public void setDestination(TableDestination destination) {
		this.destination = destination;
	}

	public BQLEventTable(String project, String dataSetName, String tableName) {
		this.reference = createReference(project, dataSetName, tableName);
		this.partitioning = createPartitioning();
		this.destination = createDestination(reference, partitioning);
	}

	private TimePartitioning createPartitioning() {
		TimePartitioning timePartitioning = new TimePartitioning();
		timePartitioning.setType("DAY");
		return timePartitioning;
	}
	
	
	public TableSchema schema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("job_id").setType("STRING"));
		fields.add(new TableFieldSchema().setName("DISTN_DATE").setType("DATE"));
		fields.add(new TableFieldSchema().setName("CLIENT_ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("CONTACT_NAME").setType("STRING"));
		fields.add(new TableFieldSchema().setName("QUANTITY").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("CONTRACT_ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("LATITUDE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("LONGITUDE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("ELEVATION").setType("DOUBLE"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}
	
	private TableReference createReference(String project, String dataSet, String tableName) {
		TableReference reference = new TableReference();
		reference.setProjectId(project);
		reference.setDatasetId(dataSet);
		reference.setTableId(tableName);
		return reference;
		
	}
	
	private TableDestination createDestination(TableReference reference, TimePartitioning partitioning) {
		TableDestination destination = new TableDestination(reference, null, partitioning);
		return destination;
	}


}
