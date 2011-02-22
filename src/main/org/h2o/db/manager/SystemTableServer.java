package org.h2o.db.manager;

import java.net.UnknownHostException;
import java.util.Set;

import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.rpc.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.IHandler;
import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.nds.rpc.json.JSONArray;
import uk.ac.standrews.cs.nds.rpc.json.JSONBoolean;
import uk.ac.standrews.cs.nds.rpc.json.JSONInteger;
import uk.ac.standrews.cs.nds.rpc.json.JSONValue;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.NetworkUtil;

public class SystemTableServer extends ApplicationServer {

    private static final String DEFAULT_SYSTEM_TABLE_REGISTRY_KEY = null;

    private final ISystemTableMigratable system_table;
    private final H2OMarshaller marshaller;

    public SystemTableServer(final ISystemTableMigratable system_table, final int port) {

        this(system_table, port, DEFAULT_SYSTEM_TABLE_REGISTRY_KEY);
    }

    public SystemTableServer(final ISystemTableMigratable system_table, final int port, final String registry_key) {

        super.setPort(port);
        try {
            super.setLocalAddress(NetworkUtil.getLocalIPv4Address());
        }
        catch (final UnknownHostException e) {
            ErrorHandling.hardExceptionError(e, "Couldn't find local IP address.");
        }

        this.system_table = system_table;
        this.registry_key = registry_key;

        marshaller = new H2OMarshaller();
        initHandlers();
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public Marshaller getMarshaller() {

        return marshaller;
    }

    @Override
    public String getApplicationRegistryKey() {

        return registry_key;
    }

    // -------------------------------------------------------------------------------------------------------

    private void initHandlers() {

        // IMigratable operations

        handler_map.put("prepareForMigration", new IHandler() {

            // public void prepareForMigration(String newLocation) throws RPCException, MigrationException, MovedException;

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final String p0 = args.getString(0);
                system_table.prepareForMigration(p0);
                return null;
            }

        });

        // public void checkConnection() throws RPCException, MovedException;

        handler_map.put("checkConnection", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                system_table.checkConnection();
                return null;
            }
        });

        // public void completeMigration() throws RPCException, MovedException, MigrationException;

        handler_map.put("completeMigration", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                system_table.completeMigration();
                return null;
            }
        });

        // public void shutdown(boolean shutdown) throws RPCException, MovedException;

        handler_map.put("shutdown", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final boolean p0 = args.getBoolean(0);
                system_table.shutdown(p0);
                return null;
            }
        });

        // ISystemTableRemote operations

        // public IChordRemoteReference getChordReference() throws RPCException;

        handler_map.put("getChordReference", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeChordRemoteReference(system_table.getChordReference());
            }
        });

        // public TableManagerWrapper lookup(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("lookup", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                return marshaller.serializeTableManagerWrapper(system_table.lookup(p0));
            }
        });

        // public boolean exists(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("exists", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                return new JSONBoolean(system_table.exists(p0));
            }
        });

        // public boolean addTableInformation(ITableManagerRemote tableManager, TableInfo tableDetails, Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException;

        handler_map.put("addTableInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args.getJSONObject(0));
                final TableInfo p1 = marshaller.deserializeTableInfo(args.getJSONObject(1));
                final Set<DatabaseInstanceWrapper> p2 = marshaller.deserializeSetDatabaseInstanceWrapper(args.getJSONArray(2));
                return new JSONBoolean(system_table.addTableInformation(p0, p1, p2));
            }
        });

        //      public boolean removeTableInformation(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("removeTableInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                return new JSONBoolean(system_table.removeTableInformation(p0));
            }
        });

        // public int addConnectionInformation(DatabaseID databaseURL, DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException;

        handler_map.put("addConnectionInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args.getJSONObject(0));
                final DatabaseInstanceWrapper p1 = marshaller.deserializeDatabaseInstanceWrapper(args.getJSONObject(1));
                return new JSONInteger(system_table.addConnectionInformation(p0, p1));
            }
        });

        // public int getNewTableSetNumber() throws RPCException, MovedException;

        handler_map.put("getNewTableSetNumber", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONInteger(system_table.getNewTableSetNumber());
            }
        });

        //      public Set<String> getAllTablesInSchema(String schemaName) throws RPCException, MovedException;

        handler_map.put("getAllTablesInSchema", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeSetString(system_table.getAllTablesInSchema(args.getString(0)));
            }
        });

        //      public void recreateSystemTable(ISystemTableRemote otherSystemTable) throws RPCException, MovedException, SQLException;

        handler_map.put("recreateSystemTable", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final ISystemTableMigratable p0 = marshaller.deserializeISystemTableMigratable(args.getString(0));
                system_table.recreateSystemTable(p0);

                return null;
            }
        });

        //      public void recreateInMemorySystemTableFromLocalPersistedState() throws RPCException, MovedException, SQLException;

        handler_map.put("recreateInMemorySystemTableFromLocalPersistedState", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                system_table.recreateInMemorySystemTableFromLocalPersistedState();

                return null;
            }
        });

        //      public Map<DatabaseID, DatabaseInstanceWrapper> getConnectionInformation() throws RPCException, MovedException, SQLException;

        handler_map.put("getConnectionInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapDatabaseIDDatabaseInstanceWrapper(system_table.getConnectionInformation());
            }
        });

        //      public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RPCException, MovedException;

        handler_map.put("getTableManagers", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapTableInfoTableManagerWrapper(system_table.getTableManagers());
            }
        });

        //      public Map<TableInfo, Set<DatabaseID>> getReplicaLocations() throws RPCException, MovedException;

        handler_map.put("getReplicaLocations", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapTableInfoSetDatabaseID(system_table.getReplicaLocations());
            }
        });

        // public Queue<DatabaseInstanceWrapper> getAvailableMachines(ActionRequest typeOfRequest) throws RPCException, MovedException;

        handler_map.put("getAvailableMachines", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final ActionRequest p0 = marshaller.deserializeActionRequest(args.getJSONObject(0));
                return marshaller.serializeQueueDatabaseInstanceWrapper(system_table.getAvailableMachines(p0));
            }
        });

        //      public void removeAllTableInformation() throws RPCException, MovedException;

        handler_map.put("removeAllTableInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                system_table.removeAllTableInformation();
                return null;
            }
        });

        //      public IDatabaseInstanceRemote getDatabaseInstance(DatabaseID databaseURL) throws RPCException, MovedException;

        handler_map.put("getDatabaseInstance", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args.getJSONObject(0));
                return marshaller.serializeIDatabaseInstanceRemote(system_table.getDatabaseInstance(p0));
            }
        });

        //      public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException;

        handler_map.put("getDatabaseInstances", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeCollectionDatabaseInstanceWrapper(system_table.getDatabaseInstances());
            }
        });

        //      public void removeConnectionInformation(IDatabaseInstanceRemote localDatabaseInstance) throws RPCException, MovedException;

        handler_map.put("removeConnectionInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final IDatabaseInstanceRemote p0 = marshaller.deserializeIDatabaseInstanceRemote(args.getString(0));
                system_table.removeConnectionInformation(p0);
                return null;
            }
        });

        //      public Set<TableManagerWrapper> getLocalDatabaseInstances(DatabaseID localMachineLocation) throws RPCException, MovedException;

        handler_map.put("getLocalDatabaseInstances", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args.getJSONObject(0));
                return marshaller.serializeSetTableManagerWrapper(system_table.getLocalDatabaseInstances(p0));
            }
        });

        //      public void changeTableManagerLocation(ITableManagerRemote stub, TableInfo tableInfo) throws RPCException, MovedException;

        handler_map.put("changeTableManagerLocation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args.getJSONObject(0));
                final TableInfo p1 = marshaller.deserializeTableInfo(args.getJSONObject(1));
                system_table.changeTableManagerLocation(p0, p1);
                return null;
            }
        });

        //      public void addTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation, DatabaseID primaryLocation, boolean active) throws RPCException, MovedException;

        handler_map.put("addTableManagerStateReplica", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args.getJSONObject(1));
                final DatabaseID p2 = marshaller.deserializeDatabaseID(args.getJSONObject(2));
                final boolean p3 = args.getBoolean(3);
                system_table.addTableManagerStateReplica(p0, p1, p2, p3);
                return null;
            }
        });

        //      public Map<TableInfo, DatabaseID> getPrimaryLocations() throws RPCException, MovedException;

        handler_map.put("getPrimaryLocations", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapTableInfoDatabaseID(system_table.getPrimaryLocations());
            }
        });

        //      public void removeTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation) throws RPCException, MovedException;

        handler_map.put("removeTableManagerStateReplica", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args.getJSONObject(1));
                system_table.removeTableManagerStateReplica(p0, p1);
                return null;
            }
        });

        //      public ITableManagerRemote recreateTableManager(TableInfo table) throws RPCException, MovedException;

        handler_map.put("recreateTableManager", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                return marshaller.serializeITableManagerRemote(system_table.recreateTableManager(p0));
            }
        });

        //      public boolean checkTableManagerAccessibility() throws RPCException, MovedException;

        handler_map.put("checkTableManagerAccessibility", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONBoolean(system_table.checkTableManagerAccessibility());
            }
        });

        handler_map.put("getAddress", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeInetSocketAddress(system_table.getAddress());

            }
        });
    }
}
