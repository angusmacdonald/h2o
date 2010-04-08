package org.h2.h2o.util;

import java.io.Serializable;

import org.h2.util.NetUtils;

/**
 * Parsed representation of an H2 database URL. 
 * 
 * <p> An example of the URL in original form: jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseURL implements Serializable {

	private static final long serialVersionUID = 3202062668933786677L;

	private static final int DEFAULT_PORT_NUMBER = 9092;

	/**
	 * The original unedited database URL.
	 */
	private String originalURL;

	/**
	 * Original URL edited to remove localhost, and replace with the local hostname.
	 */
	private String newURL;

	/**
	 * New URL, but without <code>:sm:</code>, if that exists in the URL. This gives the class a way
	 * of comparing database instances, because the existence of <code>:sm:</code> could render a true
	 * equals comparison false.
	 */
	private String urlWithoutSM;

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

	private int rmiPort;

	public static void main (String[] args){
		//Test.
		System.out.println("First test, TCP DB:");
		DatabaseURL dburl = DatabaseURL.parseURL("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test");
		System.out.println(dburl.toString());

		System.out.println("\nSecond test, MEM DB:");
		dburl = DatabaseURL.parseURL("jdbc:h2:sm:mem:one");
		System.out.println(dburl.toString());

		System.out.println("\nThird test, Other DB:");
		dburl = DatabaseURL.parseURL("jdbc:h2:data/test/scriptSimple;LOG=1;LOCK_TIMEOUT=50");
		System.out.println(dburl.toString());

		System.out.println("\nFourth test, Tilde DB:");
		dburl = DatabaseURL.parseURL("jdbc:h2:tcp://localhost/~/test");
		System.out.println(dburl.toString());

	}

	public static DatabaseURL parseURL(String url){
		if (url == null) return null;

		String[] split = url.split("\\+");
		
		url = split[0];
		
		int rmiPort = -1;
		if (split.length == 2){
			rmiPort = Integer.parseInt(split[1]);
		}
		
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
			if (newURL.indexOf(":") < 0){
				// [example: localhost/~/test]
				hostname = newURL.substring(0, newURL.indexOf("/"));
			} else {
				hostname = newURL.substring(0, newURL.indexOf(":"));
			}
			if (hostname.equals("localhost")){
				hostname = NetUtils.getLocalAddress();
			}

			//Get port
			String portString = newURL.substring(newURL.indexOf(":")+1);
			portString = portString.substring(0, portString.indexOf("/"));

			try{
				port = new Integer(portString).intValue();
			} catch (NumberFormatException e){
				port = DEFAULT_PORT_NUMBER;
			}

			//Get DB location
			dbLocation = newURL.substring(newURL.indexOf("/")+1);
		} else if (mem){
			dbLocation = url.substring(url.indexOf(":mem:")+5);
		} else {
			//jdbc:h2:data/test/scriptSimple;LOG=1;LOCK_TIMEOUT=50
			if (url.startsWith("jdbc:h2:")){
				url = url.substring("jdbc:h2:".length());
			}

			String[] remaining = url.split(";");

			dbLocation = remaining[0];

			//XXX the rest is currently ignored.
		}

		if (hostname == null) hostname = NetUtils.getLocalAddress();

		return new DatabaseURL(url, hostname, port, dbLocation, tcp, mem, schemaManager, rmiPort);
	}

	private DatabaseURL(String originalURL, String hostname, int port, String dbLocation, boolean tcp, boolean mem, boolean schemaManager, int rmiPort){
		this.originalURL = originalURL;
		this.newURL = "jdbc:h2:" + ((schemaManager)? "sm:": "") + ((tcp)? "tcp://" + hostname + ":" + port + "/": "") + ((mem)? "mem:": "") + dbLocation;
		this.urlWithoutSM = "jdbc:h2:" + ((tcp)? "tcp://" + hostname + ":" + port + "/": "") + ((mem)? "mem:": "") + dbLocation;
		this.hostname = hostname;
		this.port = port;
		this.tcp = tcp;
		this.mem = mem;
		this.schemaManager = schemaManager;
		this.dbLocation = dbLocation;
		this.rmiPort = rmiPort;
	}



	/**
	 * @param connectionType
	 * @param machineName
	 * @param connectionPort
	 * @param dbLocation2
	 */
	public DatabaseURL(String connectionType, String hostname,
			int port, String dbLocation, boolean schemaManager) {

		this(null, hostname, port, dbLocation, connectionType.equals("tcp"), connectionType.equals("mem"), schemaManager, 0);

	}

	/**
	 * Get a slightly modified version of the original URL - if the original included 'localhost' this resolves it to the local hostname.
	 * @return the new url
	 */
	public String getURL() {
		return newURL;
	}


	/**
	 * @return
	 */
	public String getUrlMinusSM() {
		return urlWithoutSM;
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
	 * Get the location of the database with all forward slashes removed.
	 * Useful if the location is to be used as part of a transaction or file name. 
	 */
	public String getDbLocationWithoutIllegalCharacters(){
		return getDbLocation().replace("/", "_").replace("~", "_");
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


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DatabaseURL [" + dbLocation + "]";
	}

	/**
	 * @return
	 */
	public boolean isValid() {
		return (newURL != null);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (mem ? 1231 : 1237);
		result = prime * result + port;
		result = prime * result + (tcp ? 1231 : 1237);
		result = prime * result
		+ ((urlWithoutSM == null) ? 0 : urlWithoutSM.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseURL other = (DatabaseURL) obj;
		if (mem != other.mem)
			return false;
		if (port != other.port)
			return false;
		if (tcp != other.tcp)
			return false;
		if (urlWithoutSM == null) {
			if (other.urlWithoutSM != null)
				return false;
		} else if (!urlWithoutSM.equals(other.urlWithoutSM))
			return false;
		return true;
	}

	/**
	 * @param rmiPort
	 */
	public void setRMIPort(int rmiPort) {
		this.rmiPort = rmiPort;
	}
	
	public int getRMIPort(){
		return rmiPort;
	}

	/**
	 * The type of database being used.
	 * @return mem, tcp, or other
	 */
	public String getConnectionType() {
		if (port == -1 && isMem()){
			return "mem";
		} else if (port != -1 && isTcp()){
			return "tcp";
		} else {
			return "other";
		}
	}

	/**
	 * @return
	 */
	public String getURLwithRMIPort() {
		return getURL() + "+" + rmiPort;
	}



}
