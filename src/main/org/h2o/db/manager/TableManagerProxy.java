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
import org.json.JSONException;
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.JSONstream.rpc.IStreamPair;
import uk.ac.standrews.cs.nds.JSONstream.rpc.JSONReader;
import uk.ac.standrews.cs.nds.JSONstream.rpc.Marshaller;
import uk.ac.standrews.cs.nds.JSONstream.rpc.Proxy;
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
            final IStreamPair streams = startCall("isAlive");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
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
            return false; //not reached
        }

    }

    @Override
    public void prepareForMigration(final String newLocation) throws RPCException, MigrationException, MovedException {

        try {
            final IStreamPair streams = startCall("prepareForMigration");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            makeCall(streams);
            finishCall(streams);
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
            final IStreamPair streams = startCall("checkConnection");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            makeCall(streams);
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
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            makeCall(streams);
            finishCall(streams);
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
            final IStreamPair streams = startCall("shutdown");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            jw.value(shutdown);

            makeCall(streams);
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
            final IStreamPair streams = startCall("IChordRemoteReference");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

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
    public TableProxy getTableProxy(final LockType lockType, final LockRequest lockRequest) throws RPCException, SQLException, MovedException {

        try {
            final IStreamPair streams = startCall("getTableProxy");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            marshaller.serializeLockType(lockType, jw);
            marshaller.serializeLockRequest(lockRequest, jw);
            final JSONReader reader = makeCall(streams);
            final TableProxy result = marshaller.deserializeTableProxy(reader);
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
            return null; // not reached
        }
    }

    @Override
    public boolean addTableInformation(final DatabaseID tableManagerURL, final TableInfo tableDetails) throws RPCException, MovedException, SQLException {

        try {
            final IStreamPair streams = startCall("addTableInformation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            marshaller.serializeDatabaseID(tableManagerURL, jw);
            marshaller.serializeTableInfo(tableDetails, jw);
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
            return false; // not reached
        }
    }

    @Override
    public void addReplicaInformation(final TableInfo tableDetails) throws RPCException, MovedException, SQLException {

        try {
            final IStreamPair streams = startCall("addReplicaInformation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            marshaller.serializeTableInfo(tableDetails, jw);

            makeCall(streams);
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
    public void removeReplicaInformation(final TableInfo ti) throws RPCException, MovedException, SQLException {

        try {
            final IStreamPair streams = startCall("removeReplicaInformation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            marshaller.serializeTableInfo(ti, jw);

            makeCall(streams);
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
    public boolean removeTableInformation() throws RPCException, SQLException, MovedException {

        try {
            final IStreamPair streams = startCall("removeTableInformation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

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
            return false; // not reached
        }
    }

    @Override
    public DatabaseID getLocation() throws RPCException, MovedException {

        try {
            final IStreamPair streams = startCall("getLocation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            final JSONReader reader = makeCall(streams);
            final DatabaseID result = marshaller.deserializeDatabaseID(reader);
            finishCall(streams);
            return result;

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
            final IStreamPair streams = startCall("releaseLockAndUpdateReplicaState");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            jw.value(commit);
            marshaller.serializeLockRequest(requestingDatabase, jw);
            marshaller.serializeCollectionCommitResult(committedQueries, jw);
            jw.value(asynchronousCommit);

            makeCall(streams);
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
    public void remove(final boolean dropCommand) throws RPCException {

        try {
            final IStreamPair streams = startCall("remove");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            jw.value(dropCommand);

            makeCall(streams);
            finishCall(streams);
        }
        catch (final Exception e) {
            dealWithException(e);
        }
    }

    @Override
    public String getSchemaName() throws RPCException {

        try {
            final IStreamPair streams = startCall("getSchemaName");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final String result = reader.stringValue();
            finishCall(streams);
            return result;

        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public String getTableName() throws RPCException {

        try {
            final IStreamPair streams = startCall("getTableName");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final String result = reader.stringValue();
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return ""; // not reached
        }
    }

    @Override
    public int getTableSet() throws RPCException {

        try {
            final IStreamPair streams = startCall("getTableSet");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final int result = reader.intValue();
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }
    }

    @Override
    public void buildTableManagerState(final ITableManagerRemote oldTableManager) throws RPCException, MovedException {

        try {
            final IStreamPair streams = startCall("buildTableManagerState");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);

            marshaller.serializeITableManagerRemote(oldTableManager, jw);
            makeCall(streams);
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
    public DatabaseID getDatabaseURL() throws RPCException {

        try {
            final IStreamPair streams = startCall("getDatabaseURL");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final DatabaseID result = marshaller.deserializeDatabaseID(reader);
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; //not reached
        }
    }

    @Override
    public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RPCException, SQLException {

        try {
            final IStreamPair streams = startCall("recreateReplicaManagerState");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            jw.value(oldPrimaryDatabaseName);
            makeCall(streams);
            finishCall(streams);
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
            final IStreamPair streams = startCall("getNumberofReplicas");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final int result = reader.intValue();
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return -1; // not reached
        }
    }

    @Override
    public void persistToCompleteStartup(final TableInfo ti) throws RPCException, StartupException {

        try {
            final IStreamPair streams = startCall("persistToCompleteStartup");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            marshaller.serializeTableInfo(ti, jw);
            makeCall(streams);
            finishCall(streams);
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
            final IStreamPair streams = startCall("getTableInfo");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final TableInfo result = marshaller.deserializeTableInfo(reader);
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() throws RPCException, MovedException {

        try {
            final IStreamPair streams = startCall("getActiveReplicas");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final Map<DatabaseInstanceWrapper, Integer> result = marshaller.deserializeMapDatabaseInstanceWrapperInteger(reader);
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() throws RPCException, MovedException {

        try {
            final IStreamPair streams = startCall("getAllReplicas");
            final JSONWriter jw = streams.getJSONwriter();

            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final Map<DatabaseInstanceWrapper, Integer> result = marshaller.deserializeMapDatabaseInstanceWrapperInteger(reader);
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public DatabaseInstanceWrapper getDatabaseLocation() throws RPCException, MovedException {

        try {

            final IStreamPair streams = startCall("getDatabaseLocation");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final DatabaseInstanceWrapper result = marshaller.deserializeDatabaseInstanceWrapper(reader);
            finishCall(streams);
            return result;
        }
        catch (final Exception e) {
            dealWithException(e);
            return null; // not reached
        }
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        try {
            final IStreamPair streams = startCall("getAddress");
            final JSONWriter jw = streams.getJSONwriter();
            setUpJSONArrayForRMI(jw);
            final JSONReader reader = makeCall(streams);
            final InetSocketAddress result = marshaller.deserializeInetSocketAddress(reader);
            finishCall(streams);
            return result;
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
     * @throws JSONException 
     */
    private void setUpJSONArrayForRMI(final JSONWriter jw) throws JSONException {

        jw.value(tableName);
    }
}
