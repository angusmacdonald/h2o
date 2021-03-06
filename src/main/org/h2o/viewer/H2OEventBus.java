/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.viewer;

import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.events.Event;
import uk.ac.standrews.cs.nds.events.bus.interfaces.IEventBus;

public class H2OEventBus {

    public static final String H2O_EVENT = "H2O_EVENT";

    private static IEventBus bus = null;

    public static void setBus(final IEventBus busParam) {

        bus = busParam;
    }

    public static void publish(final H2OEvent h2oEvent) {

        if (bus == null) { return; }

        final Event event = new Event(H2O_EVENT);
        event.put(H2O_EVENT, h2oEvent);

        bus.publishEvent(event);
    }
}
