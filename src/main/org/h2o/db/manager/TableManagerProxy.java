/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010-2011 Distributed Systems Architecture Research Group *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/
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
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.nds.rpc.Proxy;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class TableManagerProxy extends Proxy implements ITableManagerRemote {

    private static final Map<TableNameSocketPair, TableManagerProxy> proxy_map;
    private final H2OMarshaller marshaller;
    private final String tableName;

    static {
        proxy_map = new HashMap<TableNameSocketPair, TableManagerProxy>();
    }

    // -------------------------------------------------------------------------------------------------------

    protected TableManagerProxy(final InetSocketAddress node_address, final String tableName) {

        super(node_address);
        marshaller = new H2OMarshaller();
        this.tableName = tableName;
    }

    // -------------------------------------------------------------------------------------------------------

    public static synchronized TableManagerProxy getProxy(final InetSocketAddress proxy_address, final String tableName) {

        final TableNameSocketPair tableSocketPair = new TableNameSocketPair(proxy_address, tableName);
        TableManagerProxy proxy = proxy_map.get(tableSocketPair);
        if (proxy == null) {
            proxy = new TableManagerProxy(proxy_address, tableName);
            proxy_map.put(tableSocketPair, proxy);
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
    public boolean isAlive() throws RPCException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("isAlive", params).getBoolean();
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
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
            makeCall("checkConnection", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
            makeCall("completeMigration", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeChordRemoteReference(makeCall("getChordReference", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public TableProxy getTableProxy(final LockType lockType, final LockRequest lockRequest) throws RPCException, SQLException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            params.put(marshaller.serializeLockType(lockType).getValue());
            params.put(marshaller.serializeLockRequest(lockRequest).getValue());
            return marshaller.deserializeTableProxy(makeCall("getTableProxy", params).getJSONObject());
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
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
            params.put(marshaller.serializeTableInfo(tableDetails).getValue());
            makeCall("addReplicaInformation", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
            params.put(marshaller.serializeTableInfo(ti).getValue());
            makeCall("removeReplicaInformation", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("removeTableInformation", params).getBoolean();
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
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeDatabaseID(makeCall("getLocation", params).getJSONObject());
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
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("getSchemaName", params).getString();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public String getTableName() throws RPCException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("getTableName", params).getString();
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public int getTableSet() throws RPCException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("getTableSet", params).getInt();
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }

    }

    @Override
    public void buildTableManagerState(final ITableManagerRemote oldTableManager) throws RPCException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            params.put(marshaller.serializeITableManagerRemote(oldTableManager));
            makeCall("buildTableManagerState", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeDatabaseID(makeCall("getDatabaseURL", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RPCException, SQLException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
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
            final JSONArray params = setUpJSONArrayForRMI();
            return makeCall("getNumberofReplicas", params).getInt();
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }
    }

    @Override
    public void persistToCompleteStartup(final TableInfo ti) throws RPCException, StartupException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            params.put(marshaller.serializeTableInfo(ti));
            makeCall("persistToCompleteStartup", params);
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
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeTableInfo(makeCall("getTableInfo", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() throws RPCException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeMapDatabaseInstanceWrapperInteger(makeCall("getActiveReplicas", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() throws RPCException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeMapDatabaseInstanceWrapperInteger(makeCall("getAllReplicas", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public DatabaseInstanceWrapper getDatabaseLocation() throws RPCException, MovedException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeDatabaseInstanceWrapper(makeCall("getDatabaseLocation", params).getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        try {
            final JSONArray params = setUpJSONArrayForRMI();
            return marshaller.deserializeInetSocketAddress(makeCall("getAddress", params).getString());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    /**
     * This proxy class must send the table name of the table manager as the first parameter call to allow for the remote server
     * to identify which table manager is being called.
     * 
     * <p>This method creates a new JSONArray with the first parameter being the table name. It is then returned so that
     * method parameters can be added.
     * @return JSONArray of length one, containing the name of this table manager's table.
     */
    private JSONArray setUpJSONArrayForRMI() {

        final JSONArray params = new JSONArray();
        params.put(tableName);
        return params;
    }

}
