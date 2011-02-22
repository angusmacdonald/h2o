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

import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.rpc.json.JSONArray;
import uk.ac.standrews.cs.nds.rpc.json.JSONObject;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class DatabaseInstanceProxy extends Proxy implements IDatabaseInstanceRemote {

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
            return (Boolean) makeCall("isAlive").getValue();
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
            final JSONArray params = new JSONArray();
            params.put(query);
            params.put(transactionName);
            params.put(commitOperation);
            return (Integer) makeCall("execute", params).getValue();
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
            final JSONArray params = new JSONArray();
            params.put(transactionName);
            return (Integer) makeCall("prepare", params).getValue();
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
            return (String) makeCall("getConnectionString").getValue();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; //not reached
        }

    }

    @Override
    public DatabaseID getURL() throws RPCException {

        try {
            return marshaller.deserializeDatabaseID((JSONObject) makeCall("getURL").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public DatabaseID getSystemTableURL() throws RPCException {

        try {
            return marshaller.deserializeDatabaseID((JSONObject) makeCall("getSystemTableURL").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public int executeUpdate(final String sql, final boolean systemTableCommand) throws RPCException, SQLException {

        try {
            final JSONArray params = new JSONArray();
            params.put(sql);
            params.put(systemTableCommand);
            return (Integer) makeCall("executeUpdate", params).getValue();
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
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeChordRemoteReference(systemTableLocation));
            params.put(marshaller.serializeDatabaseID(databaseURL));
            makeCall("setSystemTableLocation", params);
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public ITableManagerRemote findTableManagerReference(final TableInfo tableInfo, final boolean searchOnlyCache) throws RPCException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(tableInfo));
            params.put(searchOnlyCache);
            return marshaller.deserializeITableManagerRemote((JSONObject) makeCall("findTableManagerReference", params).getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public void setAlive(final boolean alive) throws RPCException {

        try {
            final JSONArray params = new JSONArray();
            params.put(alive);
            makeCall("setAlive", params);
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public ISystemTableMigratable recreateSystemTable() throws RPCException, SQLException, SystemTableAccessException {

        try {
            return marshaller.deserializeISystemTableMigratable((String) makeCall("recreateSystemTable").getValue());
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
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(tableInfo));
            params.put(marshaller.serializeDatabaseID(databaseURL));
            return (Boolean) makeCall("recreateTableManager", params).getValue();
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public boolean isSystemTable() throws RPCException {

        try {
            return (Boolean) makeCall("isSystemTable").getValue();
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public ISystemTableMigratable getSystemTable() throws RPCException {

        try {
            return marshaller.deserializeISystemTableMigratable((String) makeCall("getSystemTable").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // final not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        try {
            return marshaller.deserializeInetSocketAddress((String) makeCall("getAddress").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // final not reached
        }
    }

    @Override
    public int getChordPort() throws RPCException {

        try {
            return (Integer) makeCall("getChordPort").getValue();
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1;
        }
    }
}
