/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.command.CommandInterface;
import org.h2.command.CommandRemote;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.expression.ParameterInterface;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.ResultInterface;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.util.ByteUtils;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.NetUtils;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.Transfer;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 * The client side part of a session when using the server mode. This object communicates with a Session on the server side.
 */
public class SessionRemote extends SessionWithState implements SessionFactory, DataHandler {

    public static final int SESSION_PREPARE = 0;

    public static final int SESSION_CLOSE = 1;

    public static final int COMMAND_EXECUTE_QUERY = 2;

    public static final int COMMAND_EXECUTE_UPDATE = 3;

    public static final int COMMAND_CLOSE = 4;

    public static final int RESULT_FETCH_ROWS = 5;

    public static final int RESULT_RESET = 6;

    public static final int RESULT_CLOSE = 7;

    public static final int COMMAND_COMMIT = 8;

    public static final int CHANGE_ID = 9;

    public static final int COMMAND_GET_META_DATA = 10;

    public static final int SESSION_PREPARE_READ_PARAMS = 11;

    public static final int SESSION_SET_ID = 12;

    public static final int SESSION_CANCEL_STATEMENT = 13;

    public static final int SESSION_CHECK_KEY = 14;

    public static final int STATUS_ERROR = 0;

    public static final int STATUS_OK = 1;

    public static final int STATUS_CLOSED = 2;

    public static final int STATUS_OK_STATE_CHANGED = 3;

    private TraceSystem traceSystem;

    private Trace trace;

    private ObjectArray transferList = new ObjectArray();

    private int nextId;

    private boolean autoCommit = true;

    private CommandInterface switchOffAutoCommit;

    private ConnectionInfo connectionInfo;

    private int objectId;

    private String databaseName;

    private String cipher;

    private byte[] fileEncryptionKey;

    private final Object lobSyncObject = new Object();

    private String sessionId;

    private int clientVersion = Constants.TCP_PROTOCOL_VERSION_5;

    private boolean autoReconnect;

    private int lastReconnect;

    private SessionInterface embedded;

    private DatabaseEventListener eventListener;

    public SessionRemote() {

        // nothing to do
    }

    private SessionRemote(final ConnectionInfo ci) throws SQLException {

        connectionInfo = ci;
    }

    private Transfer initTransfer(final ConnectionInfo ci, final String db, final String server) throws IOException, SQLException {

        final Socket socket = NetUtils.createSocket(server, Constants.DEFAULT_SERVER_PORT, ci.isSSL());
        final Transfer trans = new Transfer(this);
        trans.setSocket(socket);
        trans.setSSL(ci.isSSL());
        trans.init();
        trans.writeInt(clientVersion);
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
            trans.writeInt(clientVersion);
        }
        trans.writeString(db);
        trans.writeString(ci.getOriginalURL());
        trans.writeString(ci.getUserName());
        trans.writeBytes(ci.getUserPasswordHash());
        trans.writeBytes(ci.getFilePasswordHash());
        final String[] keys = ci.getKeys();
        trans.writeInt(keys.length);
        for (final String key : keys) {
            trans.writeString(key).writeString(ci.getProperty(key));
        }
        try {
            done(trans);
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
                clientVersion = trans.readInt();
            }
        }
        catch (final SQLException e) {
            trans.close();
            throw e;
        }
        autoCommit = true;
        return trans;
    }

    @Override
    public void cancel() {

        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     * 
     * @param id
     *            the statement id
     */
    public void cancelStatement(final int id) {

        if (clientVersion <= Constants.TCP_PROTOCOL_VERSION_5) {
            // older servers don't support this feature
            return;
        }
        for (int i = 0; i < transferList.size(); i++) {
            final Transfer transfer = (Transfer) transferList.get(i);
            try {
                final Transfer trans = transfer.openNewConnection();
                trans.init();
                trans.writeInt(clientVersion);
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
                    trans.writeInt(clientVersion);
                }
                trans.writeString(null);
                trans.writeString(null);
                trans.writeString(sessionId);
                trans.writeInt(SessionRemote.SESSION_CANCEL_STATEMENT);
                trans.writeInt(id);
                trans.close();
            }
            catch (final IOException e) {
                trace.debug("Could not cancel statement", e);
            }
        }
    }

    private void switchOffAutoCommitIfCluster() throws SQLException {

        if (autoCommit && transferList.size() > 1) {
            if (switchOffAutoCommit == null) {
                switchOffAutoCommit = prepareCommand("SET AUTOCOMMIT FALSE", Integer.MAX_VALUE);
            }
            // this will call setAutoCommit(false)
            switchOffAutoCommit.executeUpdate();
            // so we need to switch it on
            autoCommit = true;
        }
    }

    public void setAutoCommit(final boolean autoCommit) {

        this.autoCommit = autoCommit;
    }

    /**
     * Calls COMMIT if the session is in cluster mode.
     */
    public void autoCommitIfCluster() throws SQLException {

        if (autoCommit && transferList != null && transferList.size() > 1) {
            // server side auto commit is off because of race conditions
            // (update set id=1 where id=0, but update set id=2 where id=0 is
            // faster)
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                final Transfer transfer = (Transfer) transferList.get(i);
                try {
                    traceOperation("COMMAND_COMMIT", 0);
                    transfer.writeInt(SessionRemote.COMMAND_COMMIT);
                    done(transfer);
                }
                catch (final IOException e) {
                    removeServer(e, i--, ++count);
                }
            }
        }
    }

    private String getFilePrefix(final String dir) {

        final StringBuilder buff = new StringBuilder();
        buff.append(dir);
        buff.append('/');
        for (int i = 0; i < databaseName.length(); i++) {
            final char ch = databaseName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            }
            else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    @Override
    public int getPowerOffCount() {

        return 0;
    }

    @Override
    public void setPowerOffCount(final int count) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public SessionInterface createSession(final ConnectionInfo ci) throws SQLException {

        return new SessionRemote(ci).connectEmbeddedOrServer(false);
    }

    private SessionInterface connectEmbeddedOrServer(final boolean openNew) throws SQLException {

        final ConnectionInfo ci = connectionInfo;
        if (ci.isRemote()) {
            connectServer(ci);
            return this;
        }
        // create the session using reflection,
        // so that the JDBC layer can be compiled without it
        final boolean autoServerMode = Boolean.valueOf(ci.getProperty("AUTO_SERVER", "false")).booleanValue();
        ConnectionInfo backup = null;
        try {
            if (autoServerMode) {
                backup = (ConnectionInfo) ci.clone();
                connectionInfo = (ConnectionInfo) ci.clone();
            }
            final SessionFactory sf = (SessionFactory) ClassUtils.loadSystemClass("org.h2.engine.SessionFactoryEmbedded").newInstance();
            if (openNew) {
                ci.setProperty("OPEN_NEW", "true");
            }
            return sf.createSession(ci);
        }
        catch (final SQLException e) {
            final int errorCode = e.getErrorCode();
            if (errorCode == ErrorCode.DATABASE_ALREADY_OPEN_1) {
                if (autoServerMode) {
                    final String serverKey = (String) ((JdbcSQLException) e).getPayload();
                    if (serverKey != null) {
                        backup.setServerKey(serverKey);
                        // OPEN_NEW must be removed now, otherwise
                        // opening a session with AUTO_SERVER fails
                        // if another connection is already open
                        backup.removeProperty("OPEN_NEW", null);
                        connectServer(backup);
                        return this;
                    }
                }
            }
            throw e;
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
    }

    private void connectServer(final ConnectionInfo ci) throws SQLException {

        String name = ci.getName();
        if (name.startsWith("//")) {
            name = name.substring("//".length());
        }
        final int idx = name.indexOf('/');
        if (idx < 0) { throw ci.getFormatException(); }
        databaseName = name.substring(idx + 1);
        final String server = name.substring(0, idx);
        traceSystem = new TraceSystem(null, false);
        try {
            final String traceLevelFile = ci.getProperty(SetTypes.TRACE_LEVEL_FILE, null);
            if (traceLevelFile != null) {
                final int level = Integer.parseInt(traceLevelFile);
                final String prefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY);
                final String file = FileUtils.createTempFile(prefix, Constants.SUFFIX_TRACE_FILE, false, false);
                traceSystem.setFileName(file);
                traceSystem.setLevelFile(level);
            }
            final String traceLevelSystemOut = ci.getProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT, null);
            if (traceLevelSystemOut != null) {
                final int level = Integer.parseInt(traceLevelSystemOut);
                traceSystem.setLevelSystemOut(level);
            }
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
        trace = traceSystem.getTrace(Trace.JDBC);
        String serverList = null;
        if (server.indexOf(',') >= 0) {
            serverList = StringUtils.quoteStringSQL(server);
            ci.setProperty("CLUSTER", serverList);
        }
        autoReconnect = Boolean.valueOf(ci.getProperty("AUTO_RECONNECT", "false")).booleanValue();
        // AUTO_SERVER implies AUTO_RECONNECT
        autoReconnect |= Boolean.valueOf(ci.getProperty("AUTO_SERVER", "false")).booleanValue();
        if (autoReconnect && serverList != null) { throw Message.getSQLException(ErrorCode.FEATURE_NOT_SUPPORTED); }
        if (autoReconnect) {
            eventListener = ci.getDatabaseEventListenerObject();
            if (eventListener == null) {
                String className = ci.getProperty("DATABASE_EVENT_LISTENER");
                if (className != null) {
                    className = StringUtils.trim(className, true, true, "'");
                    try {
                        eventListener = (DatabaseEventListener) ClassUtils.loadUserClass(className).newInstance();
                    }
                    catch (final Throwable e) {
                        throw Message.convert(e);
                    }
                }
            }
        }
        cipher = ci.getProperty("CIPHER");
        if (cipher != null) {
            fileEncryptionKey = RandomUtils.getSecureBytes(32);
        }
        final String[] servers = StringUtils.arraySplit(server, ',', true);
        final int len = servers.length;
        transferList.clear();
        // TODO cluster: support at most 2 connections
        boolean switchOffCluster = false;
        try {
            for (int i = 0; i < len; i++) {
                try {
                    final Transfer trans = initTransfer(ci, databaseName, servers[i]);
                    transferList.add(trans);
                }
                catch (final IOException e) {
                    switchOffCluster = true;
                }
            }
            checkClosed();
            if (switchOffCluster) {
                switchOffCluster();
            }
            switchOffAutoCommitIfCluster();
        }
        catch (final SQLException e) {
            traceSystem.close();
            throw e;
        }
        upgradeClientVersionIfPossible();
    }

    private void upgradeClientVersionIfPossible() {

        try {
            // TODO check if a newer client version can be used
            // not required when sending TCP_DRIVER_VERSION_6
            final CommandInterface command = prepareCommand("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?", 1);
            final ParameterInterface param = (ParameterInterface) command.getParameters().get(0);
            param.setValue(ValueString.get("info.BUILD_ID"), false);
            final ResultInterface result = command.executeQuery(1, false);
            if (result.next()) {
                final Value[] v = result.currentRow();
                final int version = v[0].getInt();
                if (version > 71) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_6;
                }
            }
            result.close();
        }
        catch (final Exception e) {
            trace.error("Error trying to upgrade client version", e);
            // ignore
        }
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
            sessionId = ByteUtils.convertBytesToString(RandomUtils.getSecureBytes(32));
            synchronized (this) {
                for (int i = 0; i < transferList.size(); i++) {
                    final Transfer transfer = (Transfer) transferList.get(i);
                    try {
                        traceOperation("SESSION_SET_ID", 0);
                        transfer.writeInt(SessionRemote.SESSION_SET_ID);
                        transfer.writeString(sessionId);
                        done(transfer);
                    }
                    catch (final Exception e) {
                        trace.error("sessionSetId", e);
                    }
                }
            }

        }
    }

    private void switchOffCluster() throws SQLException {

        final CommandInterface ci = prepareCommand("SET CLUSTER ''", Integer.MAX_VALUE);
        ci.executeUpdate();
    }

    /**
     * Remove a server from the list of cluster nodes and disables the cluster mode.
     * 
     * @param e
     *            the exception (used for debugging)
     * @param i
     *            the index of the server to remove
     * @param count
     *            the retry count index
     */
    public void removeServer(final IOException e, final int i, final int count) throws SQLException {

        transferList.remove(i);
        if (autoReconnect(count)) { return; }
        checkClosed();
        switchOffCluster();
    }

    @Override
    public CommandInterface prepareCommand(final String sql, final int fetchSize) throws SQLException {

        synchronized (this) {
            checkClosed();
            return new CommandRemote(this, transferList, sql, fetchSize);
        }
    }

    /**
     * Automatically re-connect if necessary and if configured to do so.
     * 
     * @param count
     *            the retry count index
     * @return true if reconnected
     */
    public boolean autoReconnect(final int count) throws SQLException {

        if (!isClosed()) { return false; }
        if (!autoReconnect || !autoCommit) { return false; }
        if (count > SysProperties.MAX_RECONNECT) { return false; }
        lastReconnect++;
        embedded = connectEmbeddedOrServer(false);
        if (embedded == this) {
            // connected to a server somewhere else
            embedded = null;
        }
        else {
            // opened an embedded connection now -
            // must connect to this database in server mode
            // unfortunately
            connectEmbeddedOrServer(true);
        }
        recreateSessionState();
        if (eventListener != null) {
            eventListener.setProgress(DatabaseEventListener.STATE_RECONNECTED, databaseName, count, SysProperties.MAX_RECONNECT);
        }
        return true;
    }

    /**
     * Check if this session is closed and throws an exception if so.
     * 
     * @throws SQLException
     *             if the session is closed
     */
    public void checkClosed() throws SQLException {

        if (isClosed()) { throw new SQLException("Could not connect to database instance specified (remote session or database is closed): " + databaseName); }
    }

    @Override
    public void close() throws SQLException {

        if (transferList != null) {
            synchronized (this) {
                for (int i = 0; i < transferList.size(); i++) {
                    final Transfer transfer = (Transfer) transferList.get(i);
                    try {
                        traceOperation("SESSION_CLOSE", 0);
                        transfer.writeInt(SessionRemote.SESSION_CLOSE);
                        done(transfer);
                        transfer.close();
                    }
                    catch (final Exception e) {
                        trace.error("close", e);
                    }
                }
            }
            transferList = null;
        }
        traceSystem.close();
        if (embedded != null) {
            embedded.close();
            embedded = null;
        }
    }

    @Override
    public Trace getTrace() {

        return traceSystem.getTrace(Trace.JDBC);
    }

    public int getNextId() {

        return nextId++;
    }

    public int getCurrentId() {

        return nextId;
    }

    /**
     * Called to flush the output after data has been sent to the server and just before receiving data. This method also reads the status
     * code from the server and throws any exception the server sent.
     * 
     * @param transfer
     *            the transfer object
     * @throws SQLException
     *             if the server sent an exception
     * @throws IOException
     *             if there is a communication problem between client and server
     */
    public void done(final Transfer transfer) throws SQLException, IOException {

        transfer.flush();
        final int status = transfer.readInt();
        if (status == STATUS_ERROR) {
            final String sqlstate = transfer.readString();
            final String message = transfer.readString();
            final String sql = transfer.readString();
            final int errorCode = transfer.readInt();
            final String stackTrace = transfer.readString();
            throw new JdbcSQLException(message, sql, sqlstate, errorCode, null, stackTrace);
        }
        else if (status == STATUS_CLOSED) {
            transferList = null;
        }
        else if (status == STATUS_OK_STATE_CHANGED) {
            sessionStateChanged = true;
        }
    }

    /**
     * Returns true if the connection is in cluster mode.
     * 
     * @return true if it is
     */
    public boolean isClustered() {

        return transferList.size() > 1;
    }

    @Override
    public boolean isClosed() {

        return transferList == null || transferList.size() == 0;
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     * 
     * @param operation
     *            the operation performed
     * @param id
     *            the id of the operation
     */
    public void traceOperation(final String operation, final int id) {

        if (trace.isDebugEnabled()) {
            trace.debug(operation + " " + id);
        }
    }

    @Override
    public int allocateObjectId(final boolean needFresh, final boolean dataFile) {

        return objectId++;
    }

    @Override
    public void checkPowerOff() {

        // ok
    }

    @Override
    public void checkWritingAllowed() {

        // ok
    }

    @Override
    public int compareTypeSave(final Value a, final Value b) {

        throw Message.throwInternalError();
    }

    @Override
    public String createTempFile() throws SQLException {

        try {
            final String prefix = getFilePrefix(System.getProperty("java.io.tmpdir"));
            return FileUtils.createTempFile(prefix, Constants.SUFFIX_TEMP_FILE, true, false);
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, databaseName);
        }
    }

    @Override
    public void freeUpDiskSpace() {

        // nothing to do
    }

    @Override
    public int getChecksum(final byte[] data, final int start, final int end) {

        return 0;
    }

    @Override
    public String getDatabasePath() {

        return "";
    }

    @Override
    public String getLobCompressionAlgorithm(final int type) {

        return null;
    }

    @Override
    public int getMaxLengthInplaceLob() {

        return Constants.DEFAULT_MAX_LENGTH_CLIENTSIDE_LOB;
    }

    @Override
    public void handleInvalidChecksum() throws SQLException {

        throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
    }

    @Override
    public FileStore openFile(final String name, final String mode, final boolean mustExist) throws SQLException {

        if (mustExist && !FileUtils.exists(name)) { throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, name); }
        FileStore store;
        if (cipher == null) {
            store = FileStore.open(this, name, mode);
        }
        else {
            store = FileStore.open(this, name, mode, cipher, fileEncryptionKey, 0);
        }
        store.setCheckedWriting(false);
        try {
            store.init();
        }
        catch (final SQLException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    @Override
    public DataHandler getDataHandler() {

        return this;
    }

    @Override
    public Object getLobSyncObject() {

        return lobSyncObject;
    }

    @Override
    public boolean getLobFilesInDirectories() {

        return false;
    }

    @Override
    public SmallLRUCache getLobFileListCache() {

        return null;
    }

    public int getClientVersion() {

        return clientVersion;
    }

    public int getLastReconnect() {

        return lastReconnect;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {

        return TempFileDeleter.getInstance();
    }

    @Override
    public boolean isReconnectNeeded() {

        return false;
    }

    @Override
    public SessionInterface reconnect() {

        return this;
    }

}
