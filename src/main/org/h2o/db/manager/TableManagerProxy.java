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

    private static final Map<InetSocketAddress, TableManagerProxy> proxy_map;
    private final H2OMarshaller marshaller;

    static {
        proxy_map = new HashMap<InetSocketAddress, TableManagerProxy>();
    }

    // -------------------------------------------------------------------------------------------------------

    protected TableManagerProxy(final InetSocketAddress node_address) {

        super(node_address);
        marshaller = new H2OMarshaller();
    }

    // -------------------------------------------------------------------------------------------------------

    public static synchronized TableManagerProxy getProxy(final InetSocketAddress proxy_address) {

        TableManagerProxy proxy = proxy_map.get(proxy_address);
        if (proxy == null) {
            proxy = new TableManagerProxy(proxy_address);
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

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() throws RPCException, MovedException {

        try {
            return marshaller.deserializeMapDatabaseInstanceWrapperInteger(makeCall("getActiveReplicas").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() throws RPCException, MovedException {

        try {
            return marshaller.deserializeMapDatabaseInstanceWrapperInteger(makeCall("getAllReplicas").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public DatabaseInstanceWrapper getDatabaseLocation() throws RPCException, MovedException {

        try {
            return marshaller.deserializeDatabaseInstanceWrapper(makeCall("getDatabaseLocation").getJSONObject());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        try {
            return marshaller.deserializeInetSocketAddress(makeCall("getAddress").getString());
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }
}
