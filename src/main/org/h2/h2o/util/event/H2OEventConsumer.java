package org.h2.h2o.util.event;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import uk.ac.standrews.cs.nds.eventModel.EventFactory;
import uk.ac.standrews.cs.nds.eventModel.IEvent;
import uk.ac.standrews.cs.nds.eventModel.eventBus.EventBus;
import uk.ac.standrews.cs.nds.eventModel.eventBus.busInterfaces.IEventBus;
import uk.ac.standrews.cs.nds.eventModel.eventBus.busInterfaces.IEventConsumer;

public class H2OEventConsumer implements IEventConsumer  {
	IEventBus bus = new EventBus();

	FileWriter fstream;
	BufferedWriter out;

	public H2OEventConsumer(){
		try {
			fstream = new FileWriter("events.txt");
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean interested(IEvent event) {
		return event.getType().equals(EventFactory.DIAGNOSTIC_EVENT);
	}

	@Override
	public void receiveEvent(IEvent event) {
		Object obj = event.get(EventFactory.DIAGNOSTIC_EVENT_MSG);
//
//		H2OEvent h2oEvent = (H2OEvent) obj;

		try {
			out.write(obj + "\n");
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
