package org.h2o.db;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.manager.recovery.SystemTableAccessException;
import org.h2o.util.exceptions.MovedException;
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.rpc.stream.Connection;
import uk.ac.standrews.cs.nds.rpc.stream.JSONReader;
import uk.ac.standrews.cs.nds.rpc.stream.Marshaller;
import uk.ac.standrews.cs.nds.rpc.stream.StreamProxy;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class DatabaseInstanceProxy extends StreamProxy implements IDatabaseInstanceRemote {

    private static final Map<InetSocketAddress, DatabaseInstanceProxy> proxy_map;
    private final H2OMarshaller marshaller;

    static {
        proxy_map = new HashMap<InetSocketAddress, DatabaseInstanceProxy>();
    }

    // -------------------------------------------------------------------------------------------------------

    private DatabaseInstanceProxy(final InetSocketAddress node_address) {

        super(node_address);
        marshaller = new H2OMarshaller();
    }

    // -------------------------------------------------------------------------------------------------------

    public static synchronized DatabaseInstanceProxy getProxy(final InetSocketAddress proxy_address) {

        DatabaseInstanceProxy proxy = proxy_map.get(proxy_address);
        if (proxy == null) {
            proxy = new DatabaseInstanceProxy(proxy_address);
            proxy_map.put(proxy_address, proxy);
        }
        return proxy;
    }

    public static DatabaseInstanceProxy getProxy(final DatabaseID databaseID) {

        return DatabaseInstanceProxy.getProxy(new InetSocketAddress(databaseID.getHostname(), databaseID.getRMIPort()));
    }

    public static DatabaseInstanceProxy getProxy(final String hostname, final int port) {

        return DatabaseInstanceProxy.getProxy(new InetSocketAddress(hostname, port));
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public Marshaller getMarshaller() {

        return marshaller;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public boolean isAlive() throws RPCException, MovedException {

        try {
            final Connection connection = (Connection) startCall("isAlive");
            final JSONReader reader = makeCall(connection);

            final boolean result = reader.booleanValue();

            finishCall(connection);
            return result;
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public int execute(final String query, final String transactionName, final boolean commitOperation) throws RPCException, SQLException {

        try {
            final Connection connection = (Connection) startCall("execute");

            final JSONWriter jw = connection.getJSONwriter();

            jw.value(query);
            jw.value(transactionName);
            jw.value(commitOperation);

            final JSONReader reader = makeCall(connection);

            final int result = reader.intValue();

            finishCall(connection);

            return result;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; //not reached
        }
    }

    @Override
    public int prepare(final String transactionName) throws RPCException, SQLException {

        try {
            final Connection connection = (Connection) startCall("prepare");

            final JSONWriter jw = connection.getJSONwriter();

            jw.value(transactionName);

            final JSONReader reader = makeCall(connection);

            final int result = reader.intValue();

            finishCall(connection);

            return result;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; //not reached
        }
    }

    @Override
    public String getConnectionString() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("getConnectionString");

            final JSONReader reader = makeCall(connection);

            final String result = reader.stringValue();

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; //not reached
        }
    }

    @Override
    public DatabaseID getURL() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("getURL");

            final JSONReader reader = makeCall(connection);

            final DatabaseID result = marshaller.deserializeDatabaseID(reader);

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public DatabaseID getSystemTableURL() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("getSystemTableURL");

            final JSONReader reader = makeCall(connection);

            final DatabaseID result = marshaller.deserializeDatabaseID(reader);

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public int executeUpdate(final String sql, final boolean systemTableCommand) throws RPCException, SQLException {

        try {
            final Connection connection = (Connection) startCall("executeUpdate");
            final JSONWriter jw = connection.getJSONwriter();

            jw.value(sql);
            jw.value(systemTableCommand);

            final JSONReader reader = makeCall(connection);

            final int result = reader.intValue();

            finishCall(connection);

            return result;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; //not reached
        }
    }

    @Override
    public void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseID databaseURL) throws RPCException {

        try {
            final Connection connection = (Connection) startCall("setSystemTableLocation");

            final JSONWriter jw = connection.getJSONwriter();

            marshaller.serializeChordRemoteReference(systemTableLocation, jw);
            marshaller.serializeDatabaseID(databaseURL, jw);

            handleVoidCall(makeCall(connection));

            finishCall(connection);
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public ITableManagerRemote findTableManagerReference(final TableInfo tableInfo, final boolean searchOnlyCache) throws RPCException {

        try {
            final Connection connection = (Connection) startCall("findTableManagerReference");

            final JSONWriter jw = connection.getJSONwriter();
            marshaller.serializeTableInfo(tableInfo, jw);
            jw.value(searchOnlyCache);

            final JSONReader reader = makeCall(connection);

            final ITableManagerRemote result = marshaller.deserializeITableManagerRemote(reader);

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public void setAlive(final boolean alive) throws RPCException {

        try {
            final Connection connection = (Connection) startCall("setAlive");

            final JSONWriter jw = connection.getJSONwriter();
            jw.value(alive);

            makeCall(connection);

            finishCall(connection);
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public ISystemTableMigratable recreateSystemTable() throws RPCException, SQLException, SystemTableAccessException {

        try {
            final Connection connection = (Connection) startCall("recreateSystemTable");

            final JSONReader reader = makeCall(connection);

            final ISystemTableMigratable result = marshaller.deserializeISystemTableMigratable(reader);

            finishCall(connection);

            return result;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final SystemTableAccessException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean recreateTableManager(final TableInfo tableInfo, final DatabaseID databaseURL) throws RPCException {

        try {
            final Connection connection = (Connection) startCall("recreateTableManager");

            final JSONWriter jw = connection.getJSONwriter();
            marshaller.serializeTableInfo(tableInfo, jw);
            marshaller.serializeDatabaseID(databaseURL, jw);

            final JSONReader reader = makeCall(connection);

            final boolean result = reader.booleanValue();

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public boolean isSystemTable() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("isSystemTable");

            final JSONReader reader = makeCall(connection);

            final boolean result = reader.booleanValue();

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public ISystemTableMigratable getSystemTable() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("getSystemTable");

            final JSONReader reader = makeCall(connection);

            final ISystemTableMigratable result = marshaller.deserializeISystemTableMigratable(reader);

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // final not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        return super.node_address;
    }

    @Override
    public int getChordPort() throws RPCException {

        try {
            final Connection connection = (Connection) startCall("getChordPort");

            final JSONReader reader = makeCall(connection);

            final int result = reader.intValue();

            finishCall(connection);

            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1;
        }
    }
}
