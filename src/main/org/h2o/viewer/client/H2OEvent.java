/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.viewer.client;

import java.io.Serializable;
import java.util.Date;


public class H2OEvent implements Serializable {
	
	private static final long serialVersionUID = 3398067681240079321L;
	
	public static final String DATABASE_STARTUP = "DATABASE_STARTUP";
	
	public Date time;
	
	public String database;
	
	public DatabaseStates eventType;
	
	public String eventValue;
	
	public int eventSize;
	
	public H2OEvent() {
	};
	
	public H2OEvent(String database, DatabaseStates state) {
		this(database, state, null, 0);
	}
	
	public H2OEvent(String database, DatabaseStates state, String eventValue) {
		this(database, state, eventValue, 0);
	}
	
	public H2OEvent(String database, DatabaseStates state, String eventValue, int eventSize) {
		this.time = new Date();
		this.database = database;
		
		this.eventType = state;
		this.eventValue = eventValue;
		this.eventSize = eventSize;
	}
	
	public Date getTime() {
		return time;
	}
	
	public DatabaseStates getEventType() {
		return eventType;
	}
	
	public String getEventValue() {
		return eventValue;
	}
	
	public String getDatabase() {
		return database;
	}
	
	public int getEventSize() {
		return eventSize;
	}
	
	@Override
	public String toString() {
		return "H2OEvent [time=" + time + ", database=" + database + ", eventType=" + eventType + ", eventValue=" + eventValue + "]";
	}
	
	public String getDescription() {
		return toString();
	}
}
