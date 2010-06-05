package org.h2.h2o.autonomic;

/**
 * TODO this should be a properties file.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 *
 */
public class Settings {

	/**
	 * The time in-between attempts to replicate the system table's state onto a new successor.
	 */
	public static final long REPLICATOR_SLEEP_TIME = 2000;

	/**
	 * The number of copies required for individual relations in the system.
	 */
	public static int RELATION_REPLICATION_FACTOR = 1;
	
	/**
	 * Number of copies required of the System Table's state.
	 */
	public static int SYSTEM_TABLE_REPLICATION_FACTOR = 2;
	
	/**
	 * Number of copies required of Table Manager state.
	 */
	public static final int TABLE_MANAGER_REPLICATION_FACTOR = 2;

	/**
	 * The number of times a database should attempt to connect to an active instance
	 * when previous attempts have failed due to bind exceptions. 
	 * 
	 * <p>For example, if the value of this is 100, the instance will attempt to
	 * create a server on 100 different ports before giving up.
	 */
	public static int ATTEMPTS_AFTER_BIND_EXCEPTIONS = 100;

	/**
	 * The number of attempts that an instance will make to create/join a
	 * database system. It may fail because a lock is held on creating the
	 * System Table, or because no System Table instances are active.
	 */
	public static int ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM = 20;
	
	/**
	 * Whether the system is replicating the meta-data of the System Table
	 * and Table Managers.
	 */
	public static final boolean METADATA_REPLICATION_ENABLED = true;

}
