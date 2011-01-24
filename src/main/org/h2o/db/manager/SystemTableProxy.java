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
import org.h2o.db.manager.interfaces.ISystemTableRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class SystemTableProxy extends Proxy implements ISystemTableRemote {

    private static final Map<InetSocketAddress, SystemTableProxy> proxy_map;
    private static final H2OMarshaller marshaller;

    static {
        marshaller = new H2OMarshaller();
        proxy_map = new HashMap<InetSocketAddress, SystemTableProxy>();
    }

    protected SystemTableProxy(final InetSocketAddress node_address) {

        super(node_address);
    }

    @Override
    public TableManagerWrapper lookup(final TableInfo ti) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(ti).getValue());
            return marshaller.deserializeTableManagerWrapper(makeCall("lookup", params).getJSONObject());
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean exists(final TableInfo ti) throws RPCException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeTableInfo(ti).getValue());
            return makeCall("exists", params).getBoolean();
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
            params.put(marshaller.serializeITableManagerRemote(tableManager).getValue());
            params.put(marshaller.serializeTableInfo(tableDetails).getValue());
            params.put(marshaller.serializeCollectionDatabaseInstanceWrapper(replicaLocations).getJSONArray());

            return makeCall("addTableInformation", params).getBoolean();
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
            params.put(marshaller.serializeTableInfo(ti).getValue());
            return makeCall("removeTableInformation", params).getBoolean();
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
    public int addConnectionInformation(final DatabaseID databaseURL, final DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException {

        try {

            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeDatabaseID(databaseURL).getValue());
            params.put(marshaller.serializeDatabaseInstanceWrapper(databaseInstanceWrapper).getValue());
            return makeCall("addConnectionInformation", params).getInt();
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
            return makeCall("getNewTableSetNumber").getInt();
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
            return marshaller.deserializeSetString(makeCall("getAllTablesInSchema", params).getJSONArray());

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
    public void buildSystemTableState(final ISystemTable otherSystemTable) throws RPCException, MovedException, SQLException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeISystemTable(otherSystemTable).getValue());
            makeCall("buildSystemTableState", params);
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
    public void buildSystemTableState() throws RPCException, MovedException, SQLException {

        try {
            makeCall("buildSystemTableState");
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
            return marshaller.deserializeMapDatabaseIDDatabaseInstanceWrapper(makeCall("getConnectionInformation").getJSONObject());
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
            return marshaller.deserializeMapTableInfoTableManagerWrapper(makeCall("getTableManagers").getJSONObject());
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
            return marshaller.deserializeMapTableInfoSetDatabaseID(makeCall("getReplicaLocations").getJSONObject());
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
            params.put(marshaller.serializeDatabaseID(databaseURL).getValue());
            return marshaller.deserializeIDatabaseInstanceRemote(makeCall("getDatabaseInstance", params).getJSONObject());
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
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException {

        try {
            return marshaller.deserializeCollectionDatabaseInstanceWrapper(makeCall("getDatabaseInstances").getJSONArray());
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
            params.put(marshaller.serializeDatabaseID(localMachineLocation).getValue());
            return marshaller.deserializeCollectionTableManagerWrapper(makeCall("getLocalDatabaseInstances", params).getJSONArray());
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
            params.put(marshaller.serializeITableManagerRemote(stub).getValue());
            params.put(marshaller.serializeTableInfo(tableInfo).getValue());
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
            params.put(marshaller.serializeTableInfo(table).getValue());
            params.put(marshaller.serializeDatabaseID(replicaLocation).getValue());
            params.put(marshaller.serializeDatabaseID(primaryLocation).getValue());
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
            return marshaller.deserializeMapTableInfoDatabaseID(makeCall("getPrimaryLocations").getJSONObject());
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
            params.put(marshaller.serializeTableInfo(table).getValue());
            params.put(marshaller.serializeDatabaseID(replicaLocation).getValue());
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
            params.put(marshaller.serializeTableInfo(table).getValue());
            return marshaller.deserializeITableManagerRemote(makeCall("recreateTableManager", params).getJSONObject());
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
            return makeCall("checkTableManagerAccessibility").getBoolean();
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
            params.put(marshaller.serializeActionRequest(typeOfRequest).getValue());
            return marshaller.deserializeQueueDatabaseInstanceWrapper(makeCall("getAvailableMachines", params).getJSONArray());
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
            return marshaller.deserializeChordRemoteReference(makeCall("getChordReference").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }

    }
}
