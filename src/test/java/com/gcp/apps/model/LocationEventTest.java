package com.gcp.apps.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Test;

public class LocationEventTest {

	@Test
	public void testProcessElement() {
		LocEvent.ParseLocationEventFn parseLocationFn = new LocEvent.ParseLocationEventFn();
		DoFnTester<String, LocEvent> fnTester = DoFnTester.of(parseLocationFn);
		String locationStr = "\"1862006\",20170905,\"A\",\"2012078 \",\"THE LOCAL TRADIE\",\"A \",\"4551A \",\"21\",\"Y\",+00000254.,\"40100003\",-026.826229900000,+153.113701800000,\"14 RAMSAY CRES\",,,\"PELICAN WATERS\",\"4551\",+0.00000000000000E+000,+6.00000000000000E+000,+7.66000000000000E+000,\"2017-09-04-16.25.37.000000\",+5.20000000000000E-001";
		try {
			List<LocEvent> eventsRead = fnTester.processBundle(locationStr);
			assertTrue(eventsRead.size()>0);
			assertEquals(eventsRead.get(0).getJobId(), "1862006");
		} catch (Exception e) {
			assertTrue(false);
		}
	}

}
