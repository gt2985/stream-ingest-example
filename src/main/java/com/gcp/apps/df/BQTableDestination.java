package com.gcp.apps.df;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import com.gcp.apps.model.LocEvent;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

public class BQTableDestination extends DynamicDestinations<LocEvent, String> {

	private final TableDestination destination;
	
	private final String tableId;
	
	
	public BQTableDestination(BQLEventTable eventTable) {
		destination = eventTable.getDestination();
		tableId = eventTable.getTableId();
	}

	@Override
	public String getDestination(ValueInSingleWindow<LocEvent> event) {
		String dayString = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC).toString();
        return tableId + "$" + dayString;
	}

	@Override
	public TableSchema getSchema(String arg0) {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("job_id").setType("STRING"));
		fields.add(new TableFieldSchema().setName("DISTN_DATE").setType("DATE"));
		fields.add(new TableFieldSchema().setName("CLIENT_ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("CONTACT_NAME").setType("STRING"));
		fields.add(new TableFieldSchema().setName("QUANTITY").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("CONTRACT_ID").setType("STRING"));
		fields.add(new TableFieldSchema().setName("LATITUDE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("LONGITUDE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("ELEVATION").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	@Override
	public TableDestination getTable(String arg0) {
		return destination;
	}

}
