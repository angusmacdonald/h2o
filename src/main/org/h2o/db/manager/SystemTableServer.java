package org.h2o.db.manager;

import java.net.UnknownHostException;
import java.util.Set;

import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.rpc.stream.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.stream.IHandler;
import uk.ac.standrews.cs.nds.rpc.stream.JSONReader;
import uk.ac.standrews.cs.nds.rpc.stream.Marshaller;
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
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final String p0 = args.stringValue();
                system_table.prepareForMigration(p0);
                response.value("");
            }

        });

        // public void checkConnection() throws RPCException, MovedException;

        handler_map.put("checkConnection", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                system_table.checkConnection();
                response.value("");
            }
        });

        // public void completeMigration() throws RPCException, MovedException, MigrationException;

        handler_map.put("completeMigration", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                system_table.completeMigration();
                response.value("");
            }
        });

        // public void shutdown(boolean shutdown) throws RPCException, MovedException;

        handler_map.put("shutdown", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final boolean p0 = args.booleanValue();
                system_table.shutdown(p0);
                response.value("");
            }
        });

        // ISystemTableRemote operations

        // public IChordRemoteReference getChordReference() throws RPCException;

        handler_map.put("getChordReference", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeChordRemoteReference(system_table.getChordReference(), response);
            }
        });

        // public TableManagerWrapper lookup(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("lookup", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                marshaller.serializeTableManagerWrapper(system_table.lookup(p0), response);
            }
        });

        // public boolean exists(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("exists", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                response.value(system_table.exists(p0));
            }
        });

        // public boolean addTableInformation(ITableManagerRemote tableManager, TableInfo tableDetails, Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException;

        handler_map.put("addTableInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args);
                final TableInfo p1 = marshaller.deserializeTableInfo(args);
                final Set<DatabaseInstanceWrapper> p2 = marshaller.deserializeSetDatabaseInstanceWrapper(args);
                response.value(system_table.addTableInformation(p0, p1, p2));
            }
        });

        //      public boolean removeTableInformation(TableInfo ti) throws RPCException, MovedException;

        handler_map.put("removeTableInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                response.value(system_table.removeTableInformation(p0));
            }
        });

        // public int addConnectionInformation(DatabaseID databaseURL, DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException;

        handler_map.put("addConnectionInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args);
                final DatabaseInstanceWrapper p1 = marshaller.deserializeDatabaseInstanceWrapper(args);
                response.value(system_table.addConnectionInformation(p0, p1));
            }
        });

        // public int getNewTableSetNumber() throws RPCException, MovedException;

        handler_map.put("getNewTableSetNumber", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(system_table.getNewTableSetNumber());
            }
        });

        //      public Set<String> getAllTablesInSchema(String schemaName) throws RPCException, MovedException;

        handler_map.put("getAllTablesInSchema", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeSetString(system_table.getAllTablesInSchema(args.stringValue()), response);
            }
        });

        //      public void recreateSystemTable(ISystemTableRemote otherSystemTable) throws RPCException, MovedException, SQLException;

        handler_map.put("recreateSystemTable", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final ISystemTableMigratable p0 = marshaller.deserializeISystemTableMigratable(args);
                system_table.recreateSystemTable(p0);
                response.value("");

            }
        });

        //      public void recreateInMemorySystemTableFromLocalPersistedState() throws RPCException, MovedException, SQLException;

        handler_map.put("recreateInMemorySystemTableFromLocalPersistedState", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                system_table.recreateInMemorySystemTableFromLocalPersistedState();
                response.value("");

            }
        });

        //      public Map<DatabaseID, DatabaseInstanceWrapper> getConnectionInformation() throws RPCException, MovedException, SQLException;

        handler_map.put("getConnectionInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeMapDatabaseIDDatabaseInstanceWrapper(system_table.getConnectionInformation(), response);
            }
        });

        //      public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RPCException, MovedException;

        handler_map.put("getTableManagers", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeMapTableInfoTableManagerWrapper(system_table.getTableManagers(), response);
            }
        });

        //      public Map<TableInfo, Set<DatabaseID>> getReplicaLocations() throws RPCException, MovedException;

        handler_map.put("getReplicaLocations", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeMapTableInfoSetDatabaseID(system_table.getReplicaLocations(), response);
            }
        });

        //      public void removeAllTableInformation() throws RPCException, MovedException;

        handler_map.put("removeAllTableInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                system_table.removeAllTableInformation();
                response.value("");

            }
        });

        //      public IDatabaseInstanceRemote getDatabaseInstance(DatabaseID databaseURL) throws RPCException, MovedException;

        handler_map.put("getDatabaseInstance", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args);
                marshaller.serializeIDatabaseInstanceRemote(system_table.getDatabaseInstance(p0), response);
            }
        });

        //      public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException;

        handler_map.put("getDatabaseInstances", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeCollectionDatabaseInstanceWrapper(system_table.getDatabaseInstances(), response);
            }
        });

        //      public void removeConnectionInformation(IDatabaseInstanceRemote localDatabaseInstance) throws RPCException, MovedException;

        handler_map.put("removeConnectionInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final IDatabaseInstanceRemote p0 = marshaller.deserializeIDatabaseInstanceRemote(args);
                system_table.removeConnectionInformation(p0);
                response.value("");

            }
        });

        //      public Set<TableManagerWrapper> getLocalDatabaseInstances(DatabaseID localMachineLocation) throws RPCException, MovedException;

        handler_map.put("getLocalDatabaseInstances", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args);
                marshaller.serializeSetTableManagerWrapper(system_table.getLocalDatabaseInstances(p0), response);
            }
        });

        //      public void changeTableManagerLocation(ITableManagerRemote stub, TableInfo tableInfo) throws RPCException, MovedException;

        handler_map.put("changeTableManagerLocation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args);
                final TableInfo p1 = marshaller.deserializeTableInfo(args);
                system_table.changeTableManagerLocation(p0, p1);
                response.value("");

            }
        });

        //      public void addTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation, DatabaseID primaryLocation, boolean active) throws RPCException, MovedException;

        handler_map.put("addTableManagerStateReplica", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args);
                final DatabaseID p2 = marshaller.deserializeDatabaseID(args);
                final boolean p3 = args.booleanValue();
                system_table.addTableManagerStateReplica(p0, p1, p2, p3);
                response.value("");

            }
        });

        //      public Map<TableInfo, DatabaseID> getPrimaryLocations() throws RPCException, MovedException;

        handler_map.put("getPrimaryLocations", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeMapTableInfoDatabaseID(system_table.getPrimaryLocations(), response);
            }
        });

        //      public void removeTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation) throws RPCException, MovedException;

        handler_map.put("removeTableManagerStateReplica", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args);
                system_table.removeTableManagerStateReplica(p0, p1);
                response.value("");

            }
        });

        //      public ITableManagerRemote recreateTableManager(TableInfo table) throws RPCException, MovedException;

        handler_map.put("recreateTableManager", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                marshaller.serializeITableManagerRemote(system_table.recreateTableManager(p0), response);
            }
        });

        //      public boolean checkTableManagerAccessibility() throws RPCException, MovedException;

        handler_map.put("checkTableManagerAccessibility", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(system_table.checkTableManagerAccessibility());
            }
        });

        handler_map.put("getAddress", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeInetSocketAddress(system_table.getAddress(), response);

            }
        });

        handler_map.put("addMonitoringSummary", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final MachineMonitoringData p0 = marshaller.deserializeMachineMonitoringData(args);
                system_table.addMonitoringSummary(p0);
                response.value("");

            }
        });

        handler_map.put("getRankedListOfInstances", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeCollectionDatabaseInstanceWrapper(system_table.getRankedListOfInstances(), response);

            }
        });

        handler_map.put("getRankedListOfInstances", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final IMetric p0 = marshaller.deserializeMetric(args);

                marshaller.serializeCollectionDatabaseInstanceWrapper(system_table.getRankedListOfInstances(p0), response);

            }
        });
    }
}
