/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import org.h2.api.DatabaseEventListener;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.util.ByteUtils;
import org.h2.util.FileUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.RandomUtils;
import org.h2.util.Resources;
import org.h2.util.SortedProperties;
import org.h2.util.Tool;

/**
 * The web server is a simple standalone HTTP server that implements the H2 Console application. It is not optimized for performance.
 */
public class WebServer implements Service {

    private static final String DEFAULT_LANGUAGE = "en";

    private static final String[][] LANGUAGES = {{"de", "Deutsch"}, {"en", "English"}, {"es", "Espa\u00f1ol"}, {"fr", "Fran\u00e7ais"}, {"hu", "Magyar"}, {"in", "Indonesia"}, {"it", "Italiano"}, {"ja", "\u65e5\u672c\u8a9e"}, {"nl", "Nederlands"}, {"pl", "Polski"},
                    {"pt_BR", "Portugu\u00eas (Brasil)"}, {"pt_PT", "Portugu\u00eas (Europeu)"}, {"ru", "\u0440\u0443\u0441\u0441\u043a\u0438\u0439"}, {"tr", "T\u00fcrk\u00e7e"}, {"uk", "\u0423\u043A\u0440\u0430\u0457\u043D\u0441\u044C\u043A\u0430"}, {"zh_CN", "\u4e2d\u6587 (\u7b80\u4f53)"},
                    {"zh_TW", "\u4e2d\u6587 (\u7e41\u9ad4)"},};

    private static final String[] GENERIC = new String[]{
    // "Generic JNDI Data Source|javax.naming.InitialContext|java:comp/env/jdbc/Test|sa",
    // "Generic Firebird Server|org.firebirdsql.jdbc.FBDriver|jdbc:firebirdsql:localhost:c:/temp/firebird/test|sysdba",
    // "Generic OneDollarDB|in.co.daffodil.db.jdbc.DaffodilDBDriver|jdbc:daffodilDB_embedded:school;path=C:/temp;create=true|sa",
    // "Generic DB2|COM.ibm.db2.jdbc.net.DB2Driver|jdbc:db2://localhost/test|" ,
    // "Generic Oracle|oracle.jdbc.driver.OracleDriver|jdbc:oracle:thin:@localhost:1521:test|scott"
    // ,
    // "Generic MS SQL Server 2000|com.microsoft.jdbc.sqlserver.SQLServerDriver|jdbc:microsoft:sqlserver://localhost:1433;DatabaseName=sqlexpress|sa",
    // "Generic MS SQL Server 2005|com.microsoft.sqlserver.jdbc.SQLServerDriver|jdbc:sqlserver://localhost;DatabaseName=test|sa",
    // "Generic PostgreSQL|org.postgresql.Driver|jdbc:postgresql:test|" ,
    // "Generic MySQL|com.mysql.jdbc.Driver|jdbc:mysql://localhost:3306/test|" ,
    // "Generic HSQLDB|org.hsqldb.jdbcDriver|jdbc:hsqldb:test;hsqldb.default_table_type=cached|sa"
    // ,
    // "Generic Derby (Server)|org.apache.derby.jdbc.ClientDriver|jdbc:derby://localhost:1527/test;create=true|sa",
    // "Generic Derby (Embedded)|org.apache.derby.jdbc.EmbeddedDriver|jdbc:derby:test;create=true|sa",
    // "Generic H2 (Server)|org.h2.Driver|jdbc:h2:tcp://localhost/~/test|sa",
    // // this will be listed on top for new installations
    // "Generic H2 (Embedded)|org.h2.Driver|jdbc:h2:~/test|sa",

    // "C: Non-System Table (Two)|org.h2.Driver|jdbc:h2:tcp://localhost:9292/db_data/two/test_db|angus",
    // "B: Non-System Table (One)|org.h2.Driver|jdbc:h2:tcp://localhost:9191/db_data/three/test_db|angus",
    // "A: System Table|org.h2.Driver|jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db|angus"
    "H2O|org.h2.Driver|[ENTER JDBC URL HERE. H2O provides a JDBC URL on startup.]|sa"};

    private static int ticker;

    /**
     * The session timeout is 30 min.
     */
    private static final long SESSION_TIMEOUT = 30 * 60 * 1000;

    // static {
    // String[] list = Locale.getISOLanguages();
    // for (int i = 0; i < list.length; i++) {
    // System.out.print(list[i] + " ");
    // }
    // String lang = new java.util.Locale("hu").
    // getDisplayLanguage(new java.util.Locale("hu"));
    // java.util.Locale.CHINESE.getDisplayLanguage(java.util.Locale.CHINESE);
    // for (int i = 0; i < lang.length(); i++) {
    // System.out.println(Integer.toHexString(lang.charAt(i)) + " ");
    // }
    // }

    // private URLClassLoader urlClassLoader;
    private String driverList;

    private int port;

    private boolean allowOthers;

    private final Set running = Collections.synchronizedSet(new HashSet());

    private boolean ssl;

    private final HashMap connInfoMap = new HashMap();

    private long lastTimeoutCheck;

    private final HashMap sessions = new HashMap();

    private final HashSet languages = new HashSet();

    private String startDateTime;

    private ServerSocket serverSocket;

    private String url;

    private ShutdownHandler shutdownHandler;

    private Thread listenerThread;

    private boolean ifExists;

    private boolean allowScript;

    private boolean trace;

    private TranslateThread translateThread;

    /**
     * Read the given file from the file system or from the resources.
     * 
     * @param file
     *            the file name
     * @return the data
     */
    byte[] getFile(final String file) throws IOException {

        trace("getFile <" + file + ">");
        final byte[] data = Resources.get("/org/h2/server/web/res/" + file);
        if (data == null) {
            trace(" null");
        }
        else {
            trace(" size=" + data.length);
        }
        return data;
    }

    /**
     * Remove this web thread from the set of running threads.
     * 
     * @param t
     *            the thread to remove
     */
    synchronized void remove(final WebThread t) {

        running.remove(t);
    }

    private String generateSessionId() {

        final byte[] buff = RandomUtils.getSecureBytes(16);
        return ByteUtils.convertBytesToString(buff);
    }

    /**
     * Get the web session object for the given session id.
     * 
     * @param sessionId
     *            the session id
     * @return the web session or null
     */
    WebSession getSession(final String sessionId) {

        final long now = System.currentTimeMillis();
        if (lastTimeoutCheck + SESSION_TIMEOUT < now) {
            final Object[] list = sessions.keySet().toArray();
            for (final Object element : list) {
                final String id = (String) element;
                final WebSession session = (WebSession) sessions.get(id);
                final Long last = (Long) session.get("lastAccess");
                if (last != null && last.longValue() + SESSION_TIMEOUT < now) {
                    trace("timeout for " + id);
                    sessions.remove(id);
                }
            }
            lastTimeoutCheck = now;
        }
        final WebSession session = (WebSession) sessions.get(sessionId);
        if (session != null) {
            session.lastAccess = System.currentTimeMillis();
        }
        return session;
    }

    /**
     * Create a new web session id and object.
     * 
     * @param hostAddr
     *            the host address
     * @return the web session object
     */
    WebSession createNewSession(final String hostAddr) {

        String newId;
        do {
            newId = generateSessionId();
        }
        while (sessions.get(newId) != null);
        final WebSession session = new WebSession(this);
        session.put("sessionId", newId);
        session.put("ip", hostAddr);
        session.put("language", DEFAULT_LANGUAGE);
        sessions.put(newId, session);
        // always read the english translation,
        // so that untranslated text appears at least in english
        readTranslations(session, DEFAULT_LANGUAGE);
        return getSession(newId);
    }

    String getStartDateTime() {

        return startDateTime;
    }

    @Override
    public void init(final String[] args) {

        // TODO web: support using a different properties file
        final Properties prop = loadProperties();
        driverList = prop.getProperty("drivers");
        port = SortedProperties.getIntProperty(prop, "webPort", Constants.DEFAULT_HTTP_PORT);
        ssl = SortedProperties.getBooleanProperty(prop, "webSSL", Constants.DEFAULT_HTTP_SSL);
        allowOthers = SortedProperties.getBooleanProperty(prop, "webAllowOthers", Constants.DEFAULT_HTTP_ALLOW_OTHERS);
        for (int i = 0; args != null && i < args.length; i++) {
            final String a = args[i];
            if ("-webPort".equals(a)) {
                port = MathUtils.decodeInt(args[++i]);
            }
            else if ("-webSSL".equals(a)) {
                if (Tool.readArgBoolean(args, i) != 0) {
                    ssl = Tool.readArgBoolean(args, i) == 1;
                    i++;
                }
                else {
                    ssl = true;
                }
            }
            else if ("-webAllowOthers".equals(a)) {
                if (Tool.readArgBoolean(args, i) != 0) {
                    allowOthers = Tool.readArgBoolean(args, i) == 1;
                    i++;
                }
                else {
                    allowOthers = true;
                }
            }
            else if ("-webScript".equals(a)) {
                allowScript = true;
            }
            else if ("-baseDir".equals(a)) {
                final String baseDir = args[++i];
                SysProperties.setBaseDir(baseDir);
            }
            else if ("-ifExists".equals(a)) {
                if (Tool.readArgBoolean(args, i) != 0) {
                    ifExists = Tool.readArgBoolean(args, i) == 1;
                    i++;
                }
                else {
                    ifExists = true;
                }
            }
            else if ("-trace".equals(a)) {
                trace = true;
            }
            else if ("-log".equals(a) && SysProperties.OLD_COMMAND_LINE_OPTIONS) {
                trace = Tool.readArgBoolean(args, i) == 1;
                i++;
            }
        }

        final SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", new Locale("en", ""));
        synchronized (format) {
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            startDateTime = format.format(new Date());
        }
        trace(startDateTime);
        for (final String[] element : LANGUAGES) {
            languages.add(element[0]);
        }
        updateURL();
    }

    @Override
    public String getURL() {

        return url;
    }

    private void updateURL() {

        url = (ssl ? "https" : "http") + "://" + NetUtils.getLocalAddress() + ":" + port;
    }

    @Override
    public void start() throws SQLException {

        serverSocket = NetUtils.createServerSocketWithRetry(port, ssl);
        port = serverSocket.getLocalPort();
        updateURL();
    }

    @Override
    public void listen() {

        listenerThread = Thread.currentThread();
        try {
            while (serverSocket != null) {
                final Socket s = serverSocket.accept();
                final WebThread c = new WebThread(s, this);
                running.add(c);
                c.start();
            }
        }
        catch (final Exception e) {
            trace(e.toString());
        }
    }

    @Override
    public boolean isRunning(final boolean traceError) {

        if (serverSocket == null) { return false; }
        try {
            final Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        }
        catch (final Exception e) {
            if (traceError) {
                traceError(e);
            }
            return false;
        }
    }

    @Override
    public void stop() {

        if (serverSocket != null) {
            try {
                serverSocket.close();
            }
            catch (final IOException e) {
                traceError(e);
            }
            serverSocket = null;
        }
        if (listenerThread != null) {
            try {
                listenerThread.join(1000);
            }
            catch (final InterruptedException e) {
                TraceSystem.traceThrowable(e);
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        ArrayList list = new ArrayList(sessions.values());
        for (int i = 0; i < list.size(); i++) {
            final WebSession session = (WebSession) list.get(i);
            session.close();
        }
        list = new ArrayList(running);
        for (int i = 0; i < list.size(); i++) {
            final WebThread c = (WebThread) list.get(i);
            try {
                c.stopNow();
                c.join(100);
            }
            catch (final Exception e) {
                traceError(e);
            }
        }
    }

    /**
     * Write trace information if trace is enabled.
     * 
     * @param s
     *            the message to write
     */
    void trace(final String s) {

        if (trace) {
            System.out.println(s);
        }
    }

    /**
     * Write the stack trace if trace is enabled.
     * 
     * @param e
     *            the exception
     */
    void traceError(final Throwable e) {

        if (trace) {
            e.printStackTrace();
        }
    }

    /**
     * Check if this language is supported / translated.
     * 
     * @param language
     *            the language
     * @return true if a translation is available
     */
    boolean supportsLanguage(final String language) {

        return languages.contains(language);
    }

    /**
     * Read the translation for this language and save them in the 'text' property of this session.
     * 
     * @param session
     *            the session
     * @param language
     *            the language
     */
    void readTranslations(final WebSession session, final String language) {

        final Properties text = new Properties();
        try {
            trace("translation: " + language);
            final byte[] trans = getFile("_text_" + language + ".properties");
            trace("  " + new String(trans));
            text.load(new ByteArrayInputStream(trans));
            // remove starting # (if not translated yet)
            for (final Object element : text.entrySet()) {
                final Entry entry = (Entry) element;
                final String value = (String) entry.getValue();
                if (value.startsWith("#")) {
                    entry.setValue(value.substring(1));
                }
            }
        }
        catch (final IOException e) {
            TraceSystem.traceThrowable(e);
        }
        session.put("text", new HashMap(text));
    }

    String[][] getLanguageArray() {

        return LANGUAGES;
    }

    ArrayList getSessions() {

        final ArrayList list = new ArrayList(sessions.values());
        for (int i = 0; i < list.size(); i++) {
            final WebSession s = (WebSession) list.get(i);
            list.set(i, s.getInfo());
        }
        return list;
    }

    @Override
    public String getType() {

        return "Web";
    }

    @Override
    public String getName() {

        return "H2 Console Server";
    }

    void setAllowOthers(final boolean b) {

        allowOthers = b;
    }

    @Override
    public boolean getAllowOthers() {

        return allowOthers;
    }

    void setSSL(final boolean b) {

        ssl = b;
    }

    void setPort(final int port) {

        this.port = port;
    }

    boolean getSSL() {

        return ssl;
    }

    @Override
    public int getPort() {

        return port;
    }

    /**
     * Get the connection information for this setting.
     * 
     * @param name
     *            the setting name
     * @return the connection information
     */
    ConnectionInfo getSetting(final String name) {

        return (ConnectionInfo) connInfoMap.get(name);
    }

    /**
     * Update a connection information setting.
     * 
     * @param info
     *            the connection information
     */
    void updateSetting(final ConnectionInfo info) {

        connInfoMap.put(info.name, info);
        info.lastAccess = ticker++;
    }

    /**
     * Remove a connection information setting from the list
     * 
     * @param name
     *            the setting to remove
     */
    void removeSetting(final String name) {

        connInfoMap.remove(name);
    }

    private String getPropertiesFileName() {

        // store the properties in the user directory
        return FileUtils.getFileInUserHome(Constants.SERVER_PROPERTIES_FILE);
    }

    private Properties loadProperties() {

        final String fileName = getPropertiesFileName();
        try {
            return SortedProperties.loadProperties(fileName);
        }
        catch (final IOException e) {
            // TODO log exception
            return new Properties();
        }
    }

    /**
     * Get the list of connection information setting names.
     * 
     * @return the connection info names
     */
    String[] getSettingNames() {

        final ArrayList list = getSettings();
        final String[] names = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            names[i] = ((ConnectionInfo) list.get(i)).name;
        }
        return names;
    }

    /**
     * Get the list of connection info objects.
     * 
     * @return the list
     */
    synchronized ArrayList getSettings() {

        final ArrayList settings = new ArrayList();
        if (connInfoMap.size() == 0) {
            final Properties prop = loadProperties();
            if (prop.size() == 0) {
                for (final String element : GENERIC) {
                    final ConnectionInfo info = new ConnectionInfo(element);
                    settings.add(info);
                    updateSetting(info);
                }
            }
            else {
                for (int i = 0;; i++) {
                    final String data = prop.getProperty(String.valueOf(i));
                    if (data == null) {
                        break;
                    }
                    final ConnectionInfo info = new ConnectionInfo(data);
                    settings.add(info);
                    updateSetting(info);
                }
            }
        }
        else {
            settings.addAll(connInfoMap.values());
        }
        sortConnectionInfo(settings);
        return settings;
    }

    private void sortConnectionInfo(final ArrayList list) {

        for (int i = 1, j; i < list.size(); i++) {
            final ConnectionInfo t = (ConnectionInfo) list.get(i);
            for (j = i - 1; j >= 0 && ((ConnectionInfo) list.get(j)).lastAccess < t.lastAccess; j--) {
                list.set(j + 1, list.get(j));
            }
            list.set(j + 1, t);
        }
    }

    /**
     * Save the settings to the properties file.
     */
    synchronized void saveSettings() {

        try {
            final Properties prop = new SortedProperties();
            if (driverList != null) {
                prop.setProperty("drivers", driverList);
            }
            prop.setProperty("webPort", String.valueOf(port));
            prop.setProperty("webAllowOthers", String.valueOf(allowOthers));
            prop.setProperty("webSSL", String.valueOf(ssl));
            final ArrayList settings = getSettings();
            final int len = settings.size();
            for (int i = 0; i < len; i++) {
                final ConnectionInfo info = (ConnectionInfo) settings.get(i);
                if (info != null) {
                    prop.setProperty(String.valueOf(len - i - 1), info.getString());
                }
            }
            final OutputStream out = FileUtils.openFileOutputStream(getPropertiesFileName(), false);
            prop.store(out, Constants.SERVER_PROPERTIES_TITLE);
            out.close();
        }
        catch (final Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }

    /**
     * Open a database connection.
     * 
     * @param driver
     *            the driver class name
     * @param url
     *            the database URL
     * @param user
     *            the user name
     * @param password
     *            the password
     * @param listener
     *            the database event listener object
     * @return the database connection
     */
    Connection getConnection(String driver, String url, final String user, final String password, final DatabaseEventListener listener) throws SQLException {

        driver = driver.trim();
        url = url.trim();
        org.h2.Driver.load();
        final Properties p = new Properties();
        p.setProperty("user", user.trim());
        p.setProperty("password", password.trim());
        if (url.startsWith("jdbc:h2:")) {
            if (ifExists) {
                url += ";IFEXISTS=TRUE";
            }
            p.put("DATABASE_EVENT_LISTENER_OBJECT", listener);
            // PostgreSQL would throw a NullPointerException
            // if it is loaded before the H2 driver
            // because it can't deal with non-String objects in the connection
            // Properties
            return org.h2.Driver.load().connect(url, p);
        }
        // try {
        // Driver dr = (Driver) urlClassLoader.
        // loadClass(driver).newInstance();
        // return dr.connect(url, p);
        // } catch(ClassNotFoundException e2) {
        // throw e2;
        // }
        return JdbcUtils.getConnection(driver, url, p);
    }

    /**
     * Shut down the web server.
     */
    void shutdown() {

        if (shutdownHandler != null) {
            shutdownHandler.shutdown();
        }
    }

    public void setShutdownHandler(final ShutdownHandler shutdownHandler) {

        this.shutdownHandler = shutdownHandler;
    }

    boolean getAllowScript() {

        return allowScript;
    }

    /**
     * Create a session with a given connection.
     * 
     * @param conn
     *            the connection
     * @return the URL of the web site to access this connection
     */
    public String addSession(final Connection conn) throws SQLException {

        final WebSession session = createNewSession("local");
        session.setShutdownServerOnDisconnect();
        session.setConnection(conn);
        session.put("url", conn.getMetaData().getURL());
        final String s = (String) session.get("sessionId");
        return url + "/frame.jsp?jsessionid=" + s;
    }

    /**
     * The translate thread reads and writes the file translation.properties once a second.
     */
    private class TranslateThread extends Thread {

        private final File file = new File("translation.properties");

        private final Map translation;

        private volatile boolean stopNow;

        TranslateThread(final Map translation) {

            this.translation = translation;
        }

        public String getFileName() {

            return file.getAbsolutePath();
        }

        public void stopNow() {

            stopNow = true;
            try {
                join();
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }

        @Override
        public void run() {

            while (!stopNow) {
                try {
                    final SortedProperties sp = new SortedProperties();
                    if (file.exists()) {
                        final InputStream in = FileUtils.openFileInputStream(file.getName());
                        sp.load(in);
                        translation.putAll(sp);
                    }
                    else {
                        final OutputStream out = FileUtils.openFileOutputStream(file.getName(), false);
                        sp.putAll(translation);
                        sp.store(out, "Translation");
                    }
                    Thread.sleep(1000);
                }
                catch (final Exception e) {
                    traceError(e);
                }
            }
        }

    }

    /**
     * Start the translation thread that reads the file once a second.
     * 
     * @param translation
     *            the translation map
     * @return the name of the file to translate
     */
    String startTranslate(final Map translation) {

        if (translateThread != null) {
            translateThread.stopNow();
        }
        translateThread = new TranslateThread(translation);
        translateThread.setDaemon(true);
        translateThread.start();
        return translateThread.getFileName();
    }

}
