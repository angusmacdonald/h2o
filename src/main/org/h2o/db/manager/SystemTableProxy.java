package org.h2o.db.manager;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.rpc.json.JSONArray;
import uk.ac.standrews.cs.nds.rpc.json.JSONObject;
import uk.ac.standrews.cs.nds.rpc.json.JSONValue;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class SystemTableProxy extends Proxy implements ISystemTableMigratable {

    private static final Map<InetSocketAddress, SystemTableProxy> proxy_map;
    private final H2OMarshaller marshaller;

    static {
        proxy_map = new HashMap<InetSocketAddress, SystemTableProxy>();
    }

    // -------------------------------------------------------------------------------------------------------

    protected SystemTableProxy(final InetSocketAddress node_address) {

        super(node_address);
        marshaller = new H2OMarshaller();
    }

    // -------------------------------------------------------------------------------------------------------

    public static synchronized SystemTableProxy getProxy(final InetSocketAddress proxy_address) {

        SystemTableProxy proxy = proxy_map.get(proxy_address);
        if (proxy == null) {
            proxy = new SystemTableProxy(proxy_address);
            proxy_map.put(proxy_address, proxy);
        }
        return proxy;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public Marshaller getMarshaller() {

        return marshaller;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public TableManagerWrapper lookup(final TableInfo ti) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(ti));
            return marshaller.deserializeTableManagerWrapper((JSONObject) makeCall("lookup", params).getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Error looking up a table manager location.");
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean exists(final TableInfo ti) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();

            params.put(marshaller.serializeTableInfo(ti));
            return (Boolean) makeCall("exists", params).getValue();
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not returned
        }
    }

    @Override
    public boolean addTableInformation(final ITableManagerRemote tableManager, final TableInfo tableDetails, final Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException {

        try {
            final JSONArray params = new JSONArray();

            params.put(marshaller.serializeITableManagerRemote(tableManager));
            params.put(marshaller.serializeTableInfo(tableDetails));
            params.put(marshaller.serializeCollectionDatabaseInstanceWrapper(replicaLocations));

            return (Boolean) makeCall("addTableInformation", params).getValue();
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not returned
        }
    }

    @Override
    public boolean removeTableInformation(final TableInfo ti) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(ti));
            return (Boolean) makeCall("removeTableInformation", params).getValue();
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
    public int addConnectionInformation(final DatabaseID databaseID, final DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException {

        try {

            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeDatabaseID(databaseID));
            params.put(marshaller.serializeDatabaseInstanceWrapper(databaseInstanceWrapper));
            return (Integer) makeCall("addConnectionInformation", params).getValue();
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not returned
        }
    }

    @Override
    public int getNewTableSetNumber() throws RPCException, MovedException {

        try {
            return (Integer) makeCall("getNewTableSetNumber").getValue();
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not returned
        }
    }

    @Override
    public Set<String> getAllTablesInSchema(final String schemaName) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(schemaName);
            return marshaller.deserializeSetString((JSONArray) makeCall("getAllTablesInSchema", params).getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not returned
        }
    }

    @Override
    public void recreateSystemTable(final ISystemTable otherSystemTable) throws RPCException, MovedException, SQLException {

        if (otherSystemTable instanceof ISystemTableMigratable) {
            final ISystemTableMigratable migratableSystemTable = (ISystemTableMigratable) otherSystemTable;

            try {
                final JSONArray params = new JSONArray();
                params.put(marshaller.serializeISystemTableMigratable(migratableSystemTable));
                makeCall("recreateSystemTable", params);
            }
            catch (final MovedException e) {
                throw e;
            }
            catch (final SQLException e) {
                throw e;
            }
            catch (final Exception e) {
                dealWithException(e);
            }
        }
        else {
            throw new RPCException("Tried to serialize an implementing class of ISystemTable that was not migratable.");
        }
    }

    @Override
    public void recreateInMemorySystemTableFromLocalPersistedState() throws RPCException, MovedException, SQLException {

        try {
            makeCall("recreateInMemorySystemTableFromLocalPersistedState");
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public Map<DatabaseID, DatabaseInstanceWrapper> getConnectionInformation() throws RPCException, MovedException, SQLException {

        try {
            return marshaller.deserializeMapDatabaseIDDatabaseInstanceWrapper((JSONObject) makeCall("getConnectionInformation").getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }

    }

    @Override
    public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RPCException, MovedException {

        try {
            return marshaller.deserializeMapTableInfoTableManagerWrapper((JSONObject) makeCall("getTableManagers").getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }

    }

    @Override
    public Map<TableInfo, Set<DatabaseID>> getReplicaLocations() throws RPCException, MovedException {

        try {
            return marshaller.deserializeMapTableInfoSetDatabaseID((JSONObject) makeCall("getReplicaLocations").getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void removeAllTableInformation() throws RPCException, MovedException {

        try {
            makeCall("removeAllTableInformation");
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public IDatabaseInstanceRemote getDatabaseInstance(final DatabaseID databaseURL) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeDatabaseID(databaseURL));
            final JSONValue returnValue = makeCall("getDatabaseInstance", params);

            if (returnValue.equals(JSONObject.NULL)) {
                return null;
            }
            else {
                return marshaller.deserializeIDatabaseInstanceRemote((String) returnValue.getValue());
            }
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            e.printStackTrace();
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException {

        try {
            return marshaller.deserializeSetDatabaseInstanceWrapper((JSONArray) makeCall("getDatabaseInstances").getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void removeConnectionInformation(final IDatabaseInstanceRemote localDatabaseInstance) throws RPCException, MovedException {

        try {
            makeCall("removeConnectionInformation", marshaller.serializeIDatabaseInstanceRemote(localDatabaseInstance));
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public Set<TableManagerWrapper> getLocalDatabaseInstances(final DatabaseID localMachineLocation) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeDatabaseID(localMachineLocation));
            return marshaller.deserializeSetTableManagerWrapper((JSONArray) makeCall("getLocalDatabaseInstances", params).getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void changeTableManagerLocation(final ITableManagerRemote stub, final TableInfo tableInfo) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeITableManagerRemote(stub));
            params.put(marshaller.serializeTableInfo(tableInfo));
            makeCall("changeTableManagerLocation", params);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public void addTableManagerStateReplica(final TableInfo table, final DatabaseID replicaLocation, final DatabaseID primaryLocation, final boolean active) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(table));
            params.put(marshaller.serializeDatabaseID(replicaLocation));
            params.put(marshaller.serializeDatabaseID(primaryLocation));
            params.put(active);
            makeCall("addTableManagerStateReplica", params);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public Map<TableInfo, DatabaseID> getPrimaryLocations() throws RPCException, MovedException {

        try {
            return marshaller.deserializeMapTableInfoDatabaseID((JSONObject) makeCall("getPrimaryLocations").getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void removeTableManagerStateReplica(final TableInfo table, final DatabaseID replicaLocation) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(table));
            params.put(marshaller.serializeDatabaseID(replicaLocation));
            makeCall("removeTableManagerStateReplica", params);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public ITableManagerRemote recreateTableManager(final TableInfo table) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(table));
            return marshaller.deserializeITableManagerRemote((JSONObject) makeCall("recreateTableManager", params).getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public boolean checkTableManagerAccessibility() throws RPCException, MovedException {

        try {
            return (Boolean) makeCall("checkTableManagerAccessibility").getValue();
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
    public Queue<DatabaseInstanceWrapper> getAvailableMachines(final ActionRequest typeOfRequest) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeActionRequest(typeOfRequest));
            return marshaller.deserializeQueueDatabaseInstanceWrapper((JSONArray) makeCall("getAvailableMachines", params).getValue());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void prepareForMigration(final String newLocation) throws RPCException, MigrationException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(newLocation);
            makeCall("prepareForMigration", params);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public void checkConnection() throws RPCException, MovedException {

        try {
            makeCall("checkConnection");
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public void completeMigration() throws RPCException, MovedException, MigrationException {

        try {
            makeCall("completeMigration");
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public void shutdown(final boolean shutdown) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(shutdown);
            makeCall("shutdown", params);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public IChordRemoteReference getChordReference() throws RPCException {

        try {
            return marshaller.deserializeChordRemoteReference((JSONObject) makeCall("getChordReference").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean isAlive() throws RPCException, MovedException {

        try {
            return (Boolean) makeCall("isAlive").getValue();
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        try {
            return marshaller.deserializeInetSocketAddress((String) makeCall("getAddress").getValue());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }
}
