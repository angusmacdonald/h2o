package org.h2o.db.id;

import java.io.File;
import java.io.Serializable;

import org.h2.util.NetUtils;
import org.h2o.H2O;

public class DatabaseURL implements Serializable {

    private static final long serialVersionUID = 7601922469431481519L;

    /**
     * The original unedited database URL.
     */
    private final String originalURL;

    /**
     * Original URL edited to remove localhost, and replace with the local hostname.
     */
    private final String newURL;

    /**
     * New URL, but without <code>:sm:</code>, if that exists in the URL. This gives the class a way of comparing database instances,
     * because the existence of <code>:sm:</code> could render a true equals comparison false.
     */
    private final String urlWithoutSM;

    /**
     * Hostname contained in the URL. If the DB is in-memory there will be no host name - this field will be set to null.
     */
    private final String hostname;

    /**
     * Port number in the URL. If the DB is in-memory there will be no port number - this field will be set to -1.
     */
    private final int port;

    /**
     * The location of the database on disk.
     */
    private final String dbLocation;

    /**
     * Whether the database is in-memory.
     */
    private final boolean mem;

    /**
     * Whether the database is open to TCP connections.
     */
    private final boolean tcp;

    /**
     * Whether the database in question is a System Table.
     */
    private final boolean systemTable;

    private int rmiPort;

    /**
     * Parse a string encoded database URL.
     * @param url The database URL to be parsed.
     * @return Parsed version of the given URL.
     */
    public static DatabaseURL parseURL(String url) {

        if (url == null) { return null; }

        final String[] split = url.split("\\+");

        url = split[0];

        int rmiPort = -1;
        if (split.length == 2) {
            rmiPort = Integer.parseInt(split[1]);
        }

        final boolean tcp = url.contains(":tcp:");
        final boolean mem = url.contains(":mem:");
        final boolean systemTable = url.contains(":sm:");

        int port = -1;
        String hostname = null;
        String dbLocation = null;
        if (tcp) {
            String newURL = url;

            newURL = newURL.substring(newURL.indexOf("tcp://") + 6);

            // Get hostname
            if (newURL.indexOf(":") < 0) {
                // [example: localhost/~/test]
                hostname = newURL.substring(0, newURL.indexOf("/"));
            }
            else {
                hostname = newURL.substring(0, newURL.indexOf(":"));
            }
            if (hostname.equals("localhost")) {
                hostname = NetUtils.getLocalAddress();
            }

            // Get port
            String portString = newURL.substring(newURL.indexOf(":") + 1);
            portString = portString.substring(0, portString.indexOf("/"));

            try {
                port = new Integer(portString).intValue();
            }
            catch (final NumberFormatException e) {
                port = H2O.DEFAULT_TCP_PORT;
            }

            // Get DB location
            dbLocation = newURL.substring(newURL.indexOf("/") + 1);
        }
        else if (mem) {
            dbLocation = url.substring(url.indexOf(":mem:") + 5);
        }
        else {
            if (url.startsWith("jdbc:h2:")) {
                url = url.substring("jdbc:h2:".length());
            }

            final String[] remaining = url.split(";");

            dbLocation = remaining[0];
        }

        if (hostname == null) {
            hostname = NetUtils.getLocalAddress();
        }

        return new DatabaseURL(url, hostname, port, dbLocation, tcp, mem, systemTable, rmiPort);
    }

    private DatabaseURL(final String originalURL, final String hostname, final int port, final String dbLocation, final boolean tcp, final boolean mem, final boolean systemTable, final int rmiPort) {

        this.originalURL = originalURL;
        newURL = "jdbc:h2:" + (tcp ? "tcp://" + hostname + ":" + port + "/" : "") + (mem ? "mem:" : "") + dbLocation;
        urlWithoutSM = "jdbc:h2:" + (tcp ? "tcp://" + hostname + ":" + port + "/" : "") + (mem ? "mem:" : "") + dbLocation;
        this.hostname = hostname;
        this.port = port;
        this.tcp = tcp;
        this.mem = mem;
        this.systemTable = systemTable;
        this.dbLocation = dbLocation;
        this.rmiPort = rmiPort;
    }

    public DatabaseURL(final String connectionType, final String hostname, final int port, final String dbLocation, final boolean systemTable) {

        this(null, hostname, port, dbLocation, connectionType.equals("tcp"), connectionType.equals("mem"), systemTable, 0);

    }

    public DatabaseURL(final String connectionType, final String hostname, final int port, final String dbLocation, final boolean systemTable, final int rmiPort) {

        this(connectionType, hostname, port, dbLocation, systemTable);
        this.rmiPort = rmiPort;
    }

    /**
     * Used to create a new database URL for a TCP-exposed database.
     * @param port  The port on which the database is listening.
     * @param database_base_directory_path The path of the database files on disk.
     * @param database_name The name of the database.
     */
    public DatabaseURL(final int port, final String database_base_directory_path, final String database_name) {

        this("tcp", NetUtils.getLocalAddress(), port, getBase(database_base_directory_path) + database_name + port, false);
    }

    /**
     * Used to create a database URL for an in-memory database.
     * @param databaseName The name of the in-memory database.
     */
    public DatabaseURL(final String databaseName) {

        this("mem", NetUtils.getLocalAddress(), 0, databaseName, false);
    }

    private static String getBase(final String database_base_directory_path) {

        String base = "";
        if (database_base_directory_path != null) {
            // Ensure one trailing forward slash.
            base = database_base_directory_path;

            if (base.endsWith("\\")) {
                base = base.substring(0, base.length() - 1);
            }
            if (!base.endsWith("/")) {
                base += "/";
            }
        }
        return base;
    }

    /**
     * Get a slightly modified version of the original URL - if the original included 'localhost' this resolves it to the local hostname.
     * 
     * @return the new url
     */
    public String getURL() {

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
     * 
     * @return the dbLocation
     */
    public String getDbLocation() {

        return dbLocation;
    }

    public String getPropertiesFilePath() {

        String path = dbLocation + ".properties";
        if (mem) {
            // Store properties files for in-memory dbs in sub-directory of working directory.
            path = H2O.DEFAULT_DATABASE_DIRECTORY_PATH + File.separator + path;
        }

        return path;
    }

    /**
     * Get the location of the database with all forward slashes removed. Useful if the location is to be used as part of a transaction or
     * file name.
     */
    public String sanitizedLocation() {

        return getDbLocation().replace("/", "_").replace("\\", "_").replace("~", "_").replace("-", "__");
    }

    /**
     * True if this is an in memory database.
     * 
     * @return the mem
     */
    public boolean isMem() {

        return mem;
    }

    /**
     * True if this database is open to TCP connections.
     * 
     * @return the tcp
     */
    public boolean isTcp() {

        return tcp;
    }

    /**
     * True if this database is a System Table.
     * 
     * @return the systemTable
     */
    public boolean isSystemTable() {

        return systemTable;
    }

    /**
     * @return the originalURL
     */
    public String getOriginalURL() {

        return originalURL;
    }

    @Override
    public String toString() {

        return "DatabaseURL [" + dbLocation + "]";
    }

    public boolean isValid() {

        return newURL != null;
    }

    public void setRMIPort(final int rmiPort) {

        this.rmiPort = rmiPort;
    }

    public int getRMIPort() {

        return rmiPort;
    }

    /**
     * The type of database being used.
     * 
     * @return mem, tcp, or other
     */
    public String getConnectionType() {

        if (port == -1 && isMem()) {
            return "mem";
        }
        else if (port != -1 && isTcp()) {
            return "tcp";
        }
        else {
            return "other";
        }
    }

    /**
     * Get the full database URL with the RMI port of this database appended.
     * @return full database url <+> RMI port.
     */
    public String getURLwithRMIPort() {

        return getURL() + "+" + rmiPort;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (mem ? 1231 : 1237);
        result = prime * result + port;
        result = prime * result + (tcp ? 1231 : 1237);
        result = prime * result + (urlWithoutSM == null ? 0 : urlWithoutSM.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final DatabaseURL other = (DatabaseURL) obj;
        if (mem != other.mem) { return false; }
        if (port != other.port) { return false; }
        if (tcp != other.tcp) { return false; }
        if (urlWithoutSM == null) {
            if (other.urlWithoutSM != null) { return false; }
        }
        else if (!urlWithoutSM.equals(other.urlWithoutSM)) { return false; }
        return true;
    }

}
