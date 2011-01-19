package org.h2o.db.manager;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class TableManagerProxy extends Proxy implements ITableManagerRemote {

    private static final Map<InetSocketAddress, TableManagerProxy> proxy_map;
    private static final H2OMarshaller marshaller;

    static {
        marshaller = new H2OMarshaller();
        proxy_map = new HashMap<InetSocketAddress, TableManagerProxy>();
    }

    protected TableManagerProxy(final InetSocketAddress node_address) {

        super(node_address);
    }

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
            return false; //not reached
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
        catch (final MigrationException e) {
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
        catch (final MigrationException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public void shutdown(final boolean shutdown) throws RPCException, MovedException {

        try {
            makeCall("shutdown");
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

    @Override
    public TableProxy getTableProxy(final LockType lockType, final LockRequest lockRequest) throws RPCException, SQLException, MovedException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeLockType(lockType).getValue());
            params.put(marshaller.serializeLockRequest(lockRequest).getValue());
            return marshaller.deserializeTableProxy(makeCall("getTableProxy", params));
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public boolean addTableInformation(final DatabaseID tableManagerURL, final TableInfo tableDetails) throws RPCException, MovedException, SQLException {

        try {
            final JSONArray params = new JSONArray();
            params.put(marshaller.serializeDatabaseID(tableManagerURL).getValue());
            params.put(marshaller.serializeTableInfo(tableDetails).getValue());
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
            return false; // not reached
        }
    }

    @Override
    public void addReplicaInformation(final TableInfo tableDetails) throws RPCException, MovedException, SQLException {

        try {
            makeCall("addReplicaInformation", marshaller.serializeTableInfo(tableDetails));
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
    public void removeReplicaInformation(final TableInfo ti) throws RPCException, MovedException, SQLException {

        try {
            makeCall("removeReplicaInformation", marshaller.serializeTableInfo(ti));
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
    public boolean removeTableInformation() throws RPCException, SQLException, MovedException {

        try {
            return makeCall("removeTableInformation").getBoolean();
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
            return false; // not reached
        }
    }

    @Override
    public DatabaseID getLocation() throws RPCException, MovedException {

        try {
            return marshaller.deserializeDatabaseID(makeCall("getLocation").getJSONObject());
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
    public void releaseLockAndUpdateReplicaState(final boolean commit, final LockRequest requestingDatabase, final Collection<CommitResult> committedQueries, final boolean asynchronousCommit) throws RPCException, MovedException, SQLException {

        try {
            final JSONArray params = new JSONArray();
            params.put(commit);
            params.put(marshaller.serializeLockRequest(requestingDatabase).getValue());
            params.put(marshaller.serializeCollectionCommitResult(committedQueries).getValue());
            params.put(asynchronousCommit);
            makeCall("releaseLockAndUpdateReplicaState", params);
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
    public void remove(final boolean dropCommand) throws RPCException {

        try {
            final JSONArray params = new JSONArray();
            params.put(dropCommand);
            makeCall("remove", params);
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public String getSchemaName() throws RPCException {

        try {
            return makeCall("getSchemaName").getString();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public String getTableName() throws RPCException {

        try {

            return makeCall("getTableName").getString();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public int getTableSet() throws RPCException {

        try {

            return makeCall("getTableSet").getInt();
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }

    }

    @Override
    public void buildTableManagerState(final ITableManagerRemote oldTableManager) throws RPCException, MovedException {

        try {
            makeCall("buildTableManagerState", marshaller.serializeITableManagerRemote(oldTableManager));
        }
        catch (final MovedException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public DatabaseID getDatabaseURL() throws RPCException {

        try {

            return marshaller.deserializeDatabaseID(makeCall("getDatabaseURL").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RPCException, SQLException {

        try {
            final JSONArray params = new JSONArray();
            params.put(oldPrimaryDatabaseName);
            makeCall("recreateReplicaManagerState", params);
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public int getNumberofReplicas() throws RPCException {

        try {
            return makeCall("getNumberofReplicas").getInt();
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }
    }

    @Override
    public void persistToCompleteStartup(final TableInfo ti) throws RPCException, StartupException {

        try {
            makeCall("persistToCompleteStartup", marshaller.serializeTableInfo(ti));
        }
        catch (final StartupException e) {
            throw e;
        }
        catch (final Exception e) {
            dealWithException(e);
        }

    }

    @Override
    public TableInfo getTableInfo() throws RPCException {

        try {
            return marshaller.deserializeTableInfo(makeCall("getTableInfo").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

}
