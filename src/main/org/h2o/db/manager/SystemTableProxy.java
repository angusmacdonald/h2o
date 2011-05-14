package org.h2o.db.manager;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;

import org.h2o.autonomic.decision.ranker.metric.Metric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
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
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.rpc.stream.IStreamPair;
import uk.ac.standrews.cs.nds.rpc.stream.JSONReader;
import uk.ac.standrews.cs.nds.rpc.stream.Marshaller;
import uk.ac.standrews.cs.nds.rpc.stream.Proxy;
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

            final IStreamPair streams = startCall("lookup");
            final JSONWriter jw = streams.getJSONwriter();

            marshaller.serializeTableInfo(ti, jw);

            final JSONReader reader = makeCall(streams);
            final TableManagerWrapper result = marshaller.deserializeTableManagerWrapper(reader);
            finishCall(streams);
            return result;
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
            final IStreamPair streams = startCall("exists");
            final JSONWriter jw = streams.getJSONwriter();

            marshaller.serializeTableInfo(ti, jw);

            final JSONReader reader = makeCall(streams);
            final boolean result = reader.booleanValue();
            finishCall(streams);
            return result;

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
            final IStreamPair streams = startCall("addTableInformation");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeITableManagerRemote(tableManager, jw);
            marshaller.serializeTableInfo(tableDetails, jw);
            marshaller.serializeCollectionDatabaseInstanceWrapper(replicaLocations, jw);
            final JSONReader reader = makeCall(streams);
            final boolean result = reader.booleanValue();
            finishCall(streams);
            return result;

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
            final IStreamPair streams = startCall("removeTableInformation");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeTableInfo(ti, jw);
            final JSONReader reader = makeCall(streams);
            final boolean result = reader.booleanValue();
            finishCall(streams);
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
    public int addConnectionInformation(final DatabaseID databaseID, final DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException {

        try {
            final IStreamPair streams = startCall("addConnectionInformation");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeDatabaseID(databaseID, jw);
            marshaller.serializeDatabaseInstanceWrapper(databaseInstanceWrapper, jw);
            final JSONReader reader = makeCall(streams);
            final int result = reader.intValue();
            finishCall(streams);
            return result;

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
            final IStreamPair streams = startCall("getNewTableSetNumber");
            final JSONReader reader = makeCall(streams);
            final int result = reader.intValue();
            finishCall(streams);
            return result;
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
            final IStreamPair streams = startCall("getAllTablesInSchema");
            final JSONWriter jw = streams.getJSONwriter();
            jw.value(schemaName);
            final JSONReader reader = makeCall(streams);
            final Set<String> result = marshaller.deserializeSetString(reader);
            finishCall(streams);
            return result;

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

                final IStreamPair streams = startCall("recreateSystemTable");
                final JSONWriter jw = streams.getJSONwriter();
                marshaller.serializeISystemTableMigratable(migratableSystemTable, jw);
                handleVoidCall(makeCall(streams));
                finishCall(streams);

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

            final IStreamPair streams = startCall("recreateInMemorySystemTableFromLocalPersistedState");
            handleVoidCall(makeCall(streams));
            finishCall(streams);

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

            final IStreamPair streams = startCall("getConnectionInformation");
            final JSONReader reader = makeCall(streams);

            final Map<DatabaseID, DatabaseInstanceWrapper> result = marshaller.deserializeMapDatabaseIDDatabaseInstanceWrapper(reader);
            finishCall(streams);
            return result;
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

            final IStreamPair streams = startCall("getTableManagers");
            final JSONReader reader = makeCall(streams);

            final Map<TableInfo, TableManagerWrapper> result = marshaller.deserializeMapTableInfoTableManagerWrapper(reader);
            finishCall(streams);
            return result;

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

            final IStreamPair streams = startCall("getReplicaLocations");
            final JSONReader reader = makeCall(streams);

            final Map<TableInfo, Set<DatabaseID>> result = marshaller.deserializeMapTableInfoSetDatabaseID(reader);
            finishCall(streams);
            return result;
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

            final IStreamPair streams = startCall("removeAllTableInformation");
            handleVoidCall(makeCall(streams));

            finishCall(streams);
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public IDatabaseInstanceRemote getDatabaseInstance(final DatabaseID databaseID) throws RPCException, MovedException {

        try {

            final IStreamPair streams = startCall("getDatabaseInstance");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeDatabaseID(databaseID, jw);

            final JSONReader reader = makeCall(streams);
            final IDatabaseInstanceRemote result = marshaller.deserializeIDatabaseInstanceRemote(reader);

            finishCall(streams);
            return result;

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

            final IStreamPair streams = startCall("getDatabaseInstances");
            final JSONReader reader = makeCall(streams);

            final Set<DatabaseInstanceWrapper> result = marshaller.deserializeSetDatabaseInstanceWrapper(reader);
            finishCall(streams);
            return result;
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

            final IStreamPair streams = startCall("removeConnectionInformation");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeIDatabaseInstanceRemote(localDatabaseInstance, jw);
            handleVoidCall(makeCall(streams));

            finishCall(streams);
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

            final IStreamPair streams = startCall("getLocalDatabaseInstances");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeDatabaseID(localMachineLocation, jw);
            final JSONReader reader = makeCall(streams);

            final Set<TableManagerWrapper> result = marshaller.deserializeSetTableManagerWrapper(reader);
            finishCall(streams);
            return result;

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

            final IStreamPair streams = startCall("changeTableManagerLocation");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeITableManagerRemote(stub, jw);
            marshaller.serializeTableInfo(tableInfo, jw);
            handleVoidCall(makeCall(streams));

            finishCall(streams);

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

            final IStreamPair streams = startCall("addTableManagerStateReplica");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeTableInfo(table, jw);
            marshaller.serializeDatabaseID(replicaLocation, jw);
            marshaller.serializeDatabaseID(primaryLocation, jw);
            jw.value(active);
            handleVoidCall(makeCall(streams));

            finishCall(streams);

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

            final IStreamPair streams = startCall("getPrimaryLocations");
            final JSONReader reader = makeCall(streams);

            final Map<TableInfo, DatabaseID> result = marshaller.deserializeMapTableInfoDatabaseID(reader);
            finishCall(streams);
            return result;
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
            final IStreamPair streams = startCall("removeTableManagerStateReplica");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeTableInfo(table, jw);
            marshaller.serializeDatabaseID(replicaLocation, jw);
            handleVoidCall(makeCall(streams));

            finishCall(streams);

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
            final IStreamPair streams = startCall("recreateTableManager");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeTableInfo(table, jw);
            final JSONReader reader = makeCall(streams);

            final ITableManagerRemote result = marshaller.deserializeITableManagerRemote(reader);
            finishCall(streams);
            return result;
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
            final IStreamPair streams = startCall("checkTableManagerAccessibility");
            final JSONReader reader = makeCall(streams);

            final boolean result = reader.booleanValue();
            finishCall(streams);
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
    public Queue<DatabaseInstanceWrapper> getAvailableMachines(final Metric typeOfRequest) throws RPCException, MovedException {

        try {

            final IStreamPair streams = startCall("getAvailableMachines");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeActionRequest(typeOfRequest, jw);
            final JSONReader reader = makeCall(streams);

            final Queue<DatabaseInstanceWrapper> result = marshaller.deserializeQueueDatabaseInstanceWrapper(reader);
            finishCall(streams);
            return result;

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
            final IStreamPair streams = startCall("prepareForMigration");
            final JSONWriter jw = streams.getJSONwriter();
            jw.value(newLocation);
            handleVoidCall(makeCall(streams));

            finishCall(streams);

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
            final IStreamPair streams = startCall("checkConnection");
            handleVoidCall(makeCall(streams));
            finishCall(streams);
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
            final IStreamPair streams = startCall("completeMigration");
            handleVoidCall(makeCall(streams));
            finishCall(streams);
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
            final IStreamPair streams = startCall("shutdown");
            final JSONWriter jw = streams.getJSONwriter();
            jw.value(shutdown);
            handleVoidCall(makeCall(streams));
            finishCall(streams);
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
            final IStreamPair streams = startCall("getChordReference");
            final JSONReader reader = makeCall(streams);
            final IChordRemoteReference result = marshaller.deserializeChordRemoteReference(reader);
            finishCall(streams);

            return result;

        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean isAlive() throws RPCException, MovedException {

        try {
            final IStreamPair streams = startCall("isAlive");
            final JSONReader reader = makeCall(streams);
            final boolean result = reader.booleanValue();
            finishCall(streams);

            return result;

        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        return super.node_address;
    }

    @Override
    public void addMonitoringSummary(final MachineMonitoringData summary) throws RPCException, MovedException {

        try {

            final IStreamPair streams = startCall("addMonitoringSummary");
            final JSONWriter jw = streams.getJSONwriter();
            marshaller.serializeMachineMonitoringData(summary, jw);
            handleVoidCall(makeCall(streams));
            finishCall(streams);

        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public SortedSet<MachineMonitoringData> getRankedListOfInstances() {

        // TODO Auto-generated method stub
        return null;
    }

}
