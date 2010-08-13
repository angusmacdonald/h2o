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
