package com.gcp.apps.df;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gcp.apps.model.LocEvent;
import com.google.maps.ElevationApi;
import com.google.maps.GeoApiContext;
import com.google.maps.PendingResult;
import com.google.maps.model.ElevationResult;
import com.google.maps.model.LatLng;

public class ElevationDoFn extends DoFn<LocEvent, LocEvent>{
	
	
	// Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ElevationDoFn.class);
    //TODO move to options
    private static final String APIKEY = "";
    

    @ProcessElement
    public void processElement(ProcessContext c) {
    	LocEvent event = c.element();
    	ElevationResult er = null;
    	LocEvent newEvent = new LocEvent();
    	try {
	    	newEvent = SerializationUtils.clone(event);
	    	newEvent.setElevation(new Float(0.00));
	    	GeoApiContext context = new GeoApiContext.Builder().apiKey(APIKEY).build();
	    	LatLng location = new LatLng(Double.parseDouble(event.getLatitude()), Double.parseDouble(event.getLongitude()));
	    	PendingResult<ElevationResult> pr = ElevationApi.getByPoint(context, location);
	    	
			er = pr.await();
	    	if (er != null && er.elevation > 0) {
	    		newEvent.setElevation(((Double)er.elevation).floatValue());
	    	} 
    	} catch (Exception e) {
			LOG.error("Error getting elevation::" + e.getMessage() + e.getCause() + e.getStackTrace());			
		}
    	c.output(newEvent);
    }
    

}
