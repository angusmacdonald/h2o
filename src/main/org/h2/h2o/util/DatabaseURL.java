package org.h2.h2o.util;

import org.h2.util.NetUtils;

/**
 * Parsed representation of an H2 database URL. 
 * 
 * <p> An example of the URL in original form: jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseURL {

	/**
	 * The original unedited database URL.
	 */
	private String originalURL;
	
	/**
	 * Original URL edited to remove localhost, and replace with the local hostname.
	 */
	private String newURL;
	
	/**
	 * Hostname contained in the URL. If the DB is in-memory there will be no host name - this field will be set to null.
	 */
	private String hostname;
	
	/**
	 * Port number in the URL. If the DB is in-memory there will be no port number - this field will be set to -1.
	 */
	private int port;

	/**
	 * The location of the database on disk.
	 */
	private String dbLocation;
	
	/**
	 * Whether the database is in-memory.
	 */
	private boolean mem;
	
	/**
	 * Whether the database is open to TCP connections.
	 */
	private boolean tcp;
	
	/**
	 * Whether the database in question is a schema manager.
	 */
	private boolean schemaManager;

	
	public static void main (String[] args){
		//Test.
		System.out.println("First test, TCP DB:");
		DatabaseURL dburl = DatabaseURL.parseURL("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test");
		System.out.println(dburl.toString());
		
		System.out.println("\nSecond test, MEM DB:");
		dburl = DatabaseURL.parseURL("jdbc:h2:sm:mem:one");
		System.out.println(dburl.toString());
	}
	
	public static DatabaseURL parseURL(String url){
		boolean tcp = (url.contains(":tcp:"));
		boolean mem = (url.contains(":mem:"));
		boolean schemaManager = (url.contains(":sm:"));

		int port = -1;
		String hostname = null;
		String dbLocation = null;
		if (tcp){
			String newURL = url;
			
			newURL = newURL.substring(newURL.indexOf("tcp://")+6);
			
			//Get hostname
			hostname = newURL.substring(0, newURL.indexOf(":"));
			
			if (hostname.equals("localhost")){
				hostname = NetUtils.getLocalAddress();
			}
			
			//Get port
			String portString = newURL.substring(newURL.indexOf(":")+1);
			portString = portString.substring(0, portString.indexOf("/"));
			port = new Integer(portString).intValue();
			
			//Get DB location
			dbLocation = newURL.substring(newURL.indexOf("/")+1);
		} else if (mem){
			dbLocation = url.substring(url.indexOf(":mem:")+5);
		}
		
		
		return new DatabaseURL(url, hostname, port, dbLocation, tcp, mem, schemaManager);
	}
	
	private DatabaseURL(String originalURL, String hostname, int port, String dbLocation, boolean tcp, boolean mem, boolean schemaManager){
		this.originalURL = originalURL;
		this.newURL = "jdbc:h2:" + ((schemaManager)? "sm:": "") + ((tcp)? "tcp://" + hostname + ":" + port + "/": "") + ((mem)? "mem:": "") + dbLocation;
		this.hostname = hostname;
		this.port = port;
		this.tcp = tcp;
		this.mem = mem;
		this.schemaManager = schemaManager;
		this.dbLocation = dbLocation;
	}
	
	
	
	/**
	 * Get a slightly modified version of the original URL - if the original included 'localhost' this resolves it to the local hostname.
	 * @return the new url
	 */
	public String getNewURL() {
		return newURL;
	}

	/**
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Get the location of the database on disk.
	 * @return the dbLocation
	 */
	public String getDbLocation() {
		return dbLocation;
	}

	/**
	 * True if this is an in memory database.
	 * @return the mem
	 */
	public boolean isMem() {
		return mem;
	}

	/**
	 * True if this database is open to TCP connections.
	 * @return the tcp
	 */
	public boolean isTcp() {
		return tcp;
	}

	/**
	 * True if this database is a schema manager.
	 * @return the schemaManager
	 */
	public boolean isSchemaManager() {
		return schemaManager;
	}

	/**
	 * @return the originalURL
	 */
	public String getOriginalURL() {
		return originalURL;
	}

	public String toString(){
		String output = "Original URL: " + originalURL;
		output += "\nNew URL: " + newURL;
		output += "\nHostname: " + hostname;
		output += "\nPort: " + port;
		output += "\nDatabase Location: " + dbLocation;
		output +="\nTCP: " + tcp;
		output +="\nMEM: " + mem;
		output +="\nSchema Manager: " + schemaManager;
		
		return output;
	}
}
