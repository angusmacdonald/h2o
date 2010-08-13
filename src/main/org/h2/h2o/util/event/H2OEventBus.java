package org.h2.h2o.util.event;

import uk.ac.standrews.cs.nds.eventModel.Event;
import uk.ac.standrews.cs.nds.eventModel.eventBus.busInterfaces.IEventBus;

public class H2OEventBus {

	private static final String H2O_EVENT = "H2O_EVENT";
	private static IEventBus bus = null;

	public static void setBus(IEventBus busParam) {
		bus = busParam;
	}

	public static void publish(H2OEvent h2oEvent){
		if (bus == null) return;
		
		Event event = new Event(H2O_EVENT);
		event.put(H2O_EVENT, h2oEvent);

		bus.publishEvent(event);
	}
}
