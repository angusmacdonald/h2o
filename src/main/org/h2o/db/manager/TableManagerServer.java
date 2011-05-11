package org.h2o.db.manager;

import java.util.Collection;

import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.rpc.stream.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.stream.IHandler;
import uk.ac.standrews.cs.nds.rpc.stream.JSONReader;
import uk.ac.standrews.cs.nds.rpc.stream.Marshaller;

public class TableManagerServer extends ApplicationServer {

    private static final String DEFAULT_TABLE_MANAGER_REGISTRY_KEY = null;

    private final ITableManagerRemote table_manager;
    private final H2OMarshaller marshaller;

    public TableManagerServer(final ITableManagerRemote table_manager) {

        this(table_manager, DEFAULT_TABLE_MANAGER_REGISTRY_KEY);
    }

    public TableManagerServer(final ITableManagerRemote table_manager, final String registry_key) {

        this.table_manager = table_manager;
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

        // public final TableProxy getTableProxy(final LockType lockType, final LockRequest lockRequest) throws RPCException, SQLException, MovedException;

        handler_map.put("getTableProxy", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final LockType p0 = marshaller.deserializeLockType(args.stringValue());
                final LockRequest p1 = marshaller.deserializeLockRequest(args);
                marshaller.serializeTableProxy(table_manager.getTableProxy(p0, p1), writer);
            }
        });

        // public final boolean addTableInformation(final DatabaseID tableManagerURL, final TableInfo tableDetails) throws RPCException, MovedException, SQLException;

        handler_map.put("addTableInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args);
                final TableInfo p1 = marshaller.deserializeTableInfo(args);
                writer.value(table_manager.addTableInformation(p0, p1));
            }
        });

        // public void addReplicaInformation(final TableInfo tableDetails) throws RPCException, MovedException, SQLException;

        handler_map.put("addReplicaInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                table_manager.addReplicaInformation(p0);
                writer.value("");
            }
        });

        //public void removeReplicaInformation(final TableInfo ti) throws RPCException, MovedException, SQLException;

        handler_map.put("removeReplicaInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                table_manager.removeReplicaInformation(p0);
                writer.value("");
            }
        });

        // public boolean removeTableInformation() throws RPCException, SQLException, MovedException;

        handler_map.put("removeTableInformation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                writer.value(table_manager.removeTableInformation());
            }
        });

        // public DatabaseID getLocation() throws RPCException, MovedException;

        handler_map.put("getLocation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeDatabaseID(table_manager.getLocation(), writer);
            }
        });

        // public void releaseLockAndUpdateReplicaState(final boolean commit, final LockRequest requestingDatabase, final Collection<CommitResult> committedQueries, final boolean asynchronousCommit) throws RPCException, MovedException, SQLException;

        handler_map.put("releaseLockAndUpdateReplicaState", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final boolean p0 = args.booleanValue();
                final LockRequest p1 = marshaller.deserializeLockRequest(args);
                final Collection<CommitResult> p2 = marshaller.deserializeCollectionCommitResult(args);
                final boolean p3 = args.booleanValue();
                table_manager.releaseLockAndUpdateReplicaState(p0, p1, p2, p3);
                writer.value("");
            }
        });

        // public void remove(final boolean dropCommand) throws RPCException;

        handler_map.put("remove", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final boolean p0 = args.booleanValue();
                table_manager.remove(p0);
                writer.value("");
            }
        });

        // public String getSchemaName() throws RPCException;

        handler_map.put("getSchemaName", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                writer.value(table_manager.getSchemaName());
            }
        });

        // public String getTableName() throws RPCException;

        handler_map.put("getTableName", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                writer.value(table_manager.getTableName());
            }
        });

        // public int getTableSet() throws RPCException;

        handler_map.put("getTableSet", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                writer.value(table_manager.getTableSet());
            }
        });

        // public void buildTableManagerState(final ITableManagerRemote oldTableManager) throws RPCException, MovedException;

        handler_map.put("buildTableManagerState", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args);
                table_manager.buildTableManagerState(p0);
                writer.value("");
            }
        });

        // public DatabaseID getDatabaseURL() throws RPCException;

        handler_map.put("getDatabaseURL", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeDatabaseID(table_manager.getDatabaseURL(), writer);
            }
        });

        // public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RPCException, SQLException;

        handler_map.put("recreateReplicaManagerState", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final String p0 = args.stringValue();
                table_manager.recreateReplicaManagerState(p0);
                writer.value("");
            }
        });

        // public int getNumberofReplicas() throws RPCException;

        handler_map.put("getNumberofReplicas", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                writer.value(table_manager.getNumberofReplicas());
            }
        });

        // public void persistToCompleteStartup(TableInfo ti) throws RPCException, StartupException;

        handler_map.put("persistToCompleteStartup", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                table_manager.persistToCompleteStartup(p0);
                writer.value("");
            }
        });

        // public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() throws RPCException, MovedException

        handler_map.put("getActiveReplicas", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeMapDatabaseInstanceWrapperInteger(table_manager.getActiveReplicas(), writer);
            }
        });

        // public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() throws RPCException, MovedException

        handler_map.put("getAllReplicas", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeMapDatabaseInstanceWrapperInteger(table_manager.getAllReplicas(), writer);
            }
        });

        // public TableInfo getTableInfo() throws RPCException

        handler_map.put("getTableInfo", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeTableInfo(table_manager.getTableInfo(), writer);
            }
        });

        handler_map.put("prepareForMigration", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                final String p0 = args.stringValue();
                table_manager.prepareForMigration(p0);
                writer.value("");

            }
        });

        handler_map.put("completeMigration", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                table_manager.completeMigration();
                writer.value("");

            }
        });

        handler_map.put("getDatabaseLocation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeDatabaseInstanceWrapper(table_manager.getDatabaseLocation(), writer);

            }
        });

        handler_map.put("getAddress", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                marshaller.serializeInetSocketAddress(table_manager.getAddress(), writer);

            }
        });

        handler_map.put("checkConnection", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter writer) throws Exception {

                table_manager.checkConnection();
                writer.value("");

            }
        });
    }
}
