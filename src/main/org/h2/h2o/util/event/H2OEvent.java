package org.h2.h2o.util.event;

import java.io.Serializable;
import java.util.Date;

import org.h2.h2o.util.DatabaseURL;

public class H2OEvent implements Serializable {

	private static final long serialVersionUID = 3398067681240079321L;

	public static final String DATABASE_STARTUP = "DATABASE_STARTUP";

	private Date time;
	
	private DatabaseURL database;
	
	private DatabaseStates eventType;
	
	private String eventValue;

	private int eventSize;
	

	public H2OEvent(DatabaseURL database, DatabaseStates databaseStartup) {
		this(database, databaseStartup, null, 0);
	}
	
	public H2OEvent(DatabaseURL database, DatabaseStates databaseStartup, String eventValue) {
		this(database, databaseStartup, eventValue, 0);
	}
	
	public H2OEvent(DatabaseURL database, DatabaseStates databaseStartup, String eventValue, int eventSize) {
		this.time = new Date();
		this.database = database;
		
		this.eventType = databaseStartup;
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

	public DatabaseURL getDatabase() {
		return database;
	}

	public int getEventSize(){
		return eventSize;
	}
	
	@Override
	public String toString() {
		return "H2OEvent [time=" + time + ", database=" + database
				+ ", eventType=" + eventType + ", eventValue=" + eventValue
				+ "]";
	}
}
