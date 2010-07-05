package org.h2.h2o.autonomic;

import java.util.Properties;

import org.h2.h2o.util.locator.DatabaseDescriptorFile;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * If any of these settings are specifed in the database descriptor file they will over-ride the settings contained within this class.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class Settings {

	private static Settings singleton = null;
	
	public static Settings getInstance() {
		if (singleton == null) {
			singleton = new Settings();
		}
		return singleton;
	}
	
	/**
	 * The time in-between attempts to replicate the system table's state onto a new successor.
	 */
	public int REPLICATOR_SLEEP_TIME = 2000;

	/**
	 * The number of copies required for individual relations in the system.
	 */
	public int RELATION_REPLICATION_FACTOR = 1;
	
	/**
	 * Number of copies required of the System Table's state.
	 */
	public int SYSTEM_TABLE_REPLICATION_FACTOR = 1;
	
	/**
	 * Number of copies required of Table Manager state.
	 */
	public int TABLE_MANAGER_REPLICATION_FACTOR = 2;

	/**
	 * The number of times a database should attempt to connect to an active instance
	 * when previous attempts have failed due to bind exceptions. 
	 * 
	 * <p>For example, if the value of this is 100, the instance will attempt to
	 * create a server on 100 different ports before giving up.
	 */
	public int ATTEMPTS_AFTER_BIND_EXCEPTIONS = 100;

	/**
	 * The number of attempts that an instance will make to create/join a
	 * database system. It may fail because a lock is held on creating the
	 * System Table, or because no System Table instances are active.
	 */
	public int ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM = 20;
	
	/**
	 * Whether the system is replicating the meta-data of the System Table
	 * and Table Managers.
	 */
	public boolean METADATA_REPLICATION_ENABLED = true;


	public void updateSettings(DatabaseDescriptorFile descriptor) {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Updating database settings from global descriptor file.");
		
		update (descriptor, REPLICATOR_SLEEP_TIME, "REPLICATOR_SLEEP_TIME");
		update (descriptor, RELATION_REPLICATION_FACTOR, "RELATION_REPLICATION_FACTOR");
		update (descriptor, SYSTEM_TABLE_REPLICATION_FACTOR, "SYSTEM_TABLE_REPLICATION_FACTOR");
		update (descriptor, TABLE_MANAGER_REPLICATION_FACTOR, "TABLE_MANAGER_REPLICATION_FACTOR");
		update (descriptor, ATTEMPTS_AFTER_BIND_EXCEPTIONS, "ATTEMPTS_AFTER_BIND_EXCEPTIONS");
		update (descriptor, ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM, "ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM");
		update (descriptor, METADATA_REPLICATION_ENABLED, "METADATA_REPLICATION_ENABLED");
	}
	

	public static void setDescriptorProperties(Properties descriptor) {
		descriptor.setProperty("REPLICATOR_SLEEP_TIME", "2000");
		descriptor.setProperty("RELATION_REPLICATION_FACTOR", "2");
		descriptor.setProperty("SYSTEM_TABLE_REPLICATION_FACTOR", "2");
		descriptor.setProperty("TABLE_MANAGER_REPLICATION_FACTOR", "2");
		descriptor.setProperty("ATTEMPTS_AFTER_BIND_EXCEPTIONS", "100");
		descriptor.setProperty("ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM", "20");
		descriptor.setProperty("METADATA_REPLICATION_ENABLED", "true");
		
	}

	private void update(DatabaseDescriptorFile descriptor, boolean field, String fieldName) {
		String value = descriptor.getProperty(fieldName);
		if (value != null) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Setting " + fieldName + " = " + value);	
			field = Boolean.parseBoolean(value);
		}
	}
	
	private void update(DatabaseDescriptorFile descriptor, int field, String name) {
		String value = descriptor.getProperty(name);
		if (value != null) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Setting " + name + " = " + value);	
			field = Integer.parseInt(value);
		}
	}
}
