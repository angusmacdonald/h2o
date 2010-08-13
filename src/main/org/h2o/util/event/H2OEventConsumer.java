/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.event;

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
