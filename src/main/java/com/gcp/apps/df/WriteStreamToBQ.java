/**
 * 
 */
package com.gcp.apps.df;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gcp.apps.model.LocEvent;
import com.google.api.services.bigquery.model.TableRow;


public class WriteStreamToBQ implements Serializable {
	
	public interface StreamOptions extends DataflowPipelineOptions{
			
			@Description("PUbSub subscription to read messages from")
			@Required
			String getSubscriptionName();
			void setSubscriptionName(String subscriptionName);
			
			@Description("BigQuery TableName")
			@Required
			String getTablename();
			void setTablename(String tablename);
			
			@Description("BigQuery Dataset name")
			@Required
			String getDataset();
			void setDataset(String dataset);
		}

	private static final Logger LOG = LoggerFactory.getLogger(WriteStreamToBQ.class);
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		WriteStreamToBQ rPBq = new WriteStreamToBQ();
		PipelineResult result = null;
		try {
			
			StreamOptions options =  PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamOptions.class);
			Pipeline pipe = rPBq.createStream(options);
			LOG.info("pipe crated::" + pipe);
			result = pipe.run(options);
		} catch (IOException e) {
			throw e;
		}
		
		LOG.info("result::state:" + result.getState());
		LOG.info("result::metrics:" + result.metrics());
	}
	
	
	public Pipeline createStream(StreamOptions options) throws IOException {

		LOG.info("options::" + options);
		Pipeline pipeL = Pipeline.create(options);

		PCollection<LocEvent> locationEvents = pipeL.apply("ReceiveMessage", 
					PubsubIO.readMessages().fromSubscription(options.getSubscriptionName()))
				.apply(Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1))))
				.apply("Get Payload", MapElements.via(new SimpleFunction<PubsubMessage, String>() {
					public String apply(PubsubMessage msg) {
						LOG.info("Message received::" + new String(msg.getPayload()));
						String payload = null;
						try {
							payload = new String(msg.getPayload());
							LOG.info("Payload from msg::" + payload);
						} catch (Exception e) {
							LOG.error("Exception while parsing msg :" + e.getMessage() + e.getCause());
							e.printStackTrace();
						}
						return payload;
					}
				}))
				.apply("ParseLocationEvent",  ParDo.of(new LocEvent.ParseLocationEventFn()));
		
		WriteResult results = locationEvents
				//uncomment to make google maps call to get elevation for latlong
				//.apply("EnrichwithElevation", ParDo.of(new ElevationFn()))
				.apply("WriteToBQ", BigQueryIO.<LocEvent>write()
						.to(new BQTableDestination(new BQLEventTable(options.getProject(), options.getDataset(), options.getTablename())))
						.withFormatFunction(new SerializableFunction<LocEvent, TableRow>() {
							
							public TableRow apply(LocEvent levent) {
								TableRow row = new TableRow();
								row.set(BQLEventTable.FIELD.job_id.toString(), levent.getJobId());
								row.set(BQLEventTable.FIELD.DISTN_DATE.toString(), levent.getSchedDate());
								row.set(BQLEventTable.FIELD.CLIENT_ID.toString(), levent.getClientId());
								row.set(BQLEventTable.FIELD.CONTACT_NAME.toString(), levent.getContactName());
								row.set(BQLEventTable.FIELD.QUANTITY.toString(), levent.getQuantity());
								row.set(BQLEventTable.FIELD.CONTRACT_ID.toString(), levent.getContractId());
								row.set(BQLEventTable.FIELD.LATITUDE.toString(), levent.getLatitude());
								row.set(BQLEventTable.FIELD.LONGITUDE.toString(), levent.getLongitude());
								row.set(BQLEventTable.FIELD.ELEVATION.toString(), levent.getElevation());
								LOG.info("row:::" + row);
								return row;
							}
						})
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		return pipeL;
	}
	
}
