package org.h2o.db;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableRemote;
import org.h2o.db.manager.recovery.SystemTableAccessException;
import org.h2o.util.exceptions.MovedException;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class DatabaseInstanceProxy extends Proxy implements IDatabaseInstanceRemote {

    private static final Map<InetSocketAddress, DatabaseInstanceProxy> proxy_map;
    private static final H2OMarshaller marshaller;

    static {
        marshaller = new H2OMarshaller();
        proxy_map = new HashMap<InetSocketAddress, DatabaseInstanceProxy>();
    }

    // -------------------------------------------------------------------------------------------------------

    public DatabaseInstanceProxy(final InetSocketAddress node_address) {

        super(node_address);
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public boolean isAlive() throws RPCException, MovedException {

        try {
            return makeCall("isAlive").getBoolean();
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
            return makeCall("execute", params).getInt();
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
            return makeCall("prepare", params).getInt();
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
            return makeCall("getConnectionString").getString();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; //not reached
        }

    }

    @Override
    public DatabaseID getURL() throws RPCException {

        try {
            return marshaller.deserializeDatabaseID(makeCall("getURL").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public DatabaseID getSystemTableURL() throws RPCException {

        try {
            return marshaller.deserializeDatabaseID(makeCall("getSystemTableURL").getJSONObject());
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
            return makeCall("executeUpdate", params).getInt();
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
            params.put(marshaller.serializeChordRemoteReference(systemTableLocation).getValue());
            params.put(marshaller.serializeDatabaseID(databaseURL).getValue());
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
            params.put(marshaller.serializeTableInfo(tableInfo).getValue());
            params.put(searchOnlyCache);
            return marshaller.deserializeITableManagerRemote(makeCall("findTableManagerReference", params).getJSONObject());
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
    public ISystemTableRemote recreateSystemTable() throws RPCException, SQLException, SystemTableAccessException {

        try {
            return marshaller.deserializeSystemTableRemote(makeCall("recreateSystemTable").getJSONObject());
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
            params.put(marshaller.serializeTableInfo(tableInfo).getValue());
            params.put(marshaller.serializeDatabaseID(databaseURL).getValue());
            return makeCall("recreateTableManager", params).getBoolean();
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public boolean isSystemTable() throws RPCException {

        try {
            return makeCall("isSystemTable").getBoolean();
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public ISystemTableRemote getSystemTable() throws RPCException {

        try {
            return marshaller.deserializeSystemTableRemote(makeCall("getSystemTable").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // final not reached
        }
    }
}
