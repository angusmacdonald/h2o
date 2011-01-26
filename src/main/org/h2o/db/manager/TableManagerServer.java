package org.h2o.db.manager;

import java.util.Collection;
import java.util.HashMap;

import org.h2o.db.H2OMarshaller;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.IHandler;
import uk.ac.standrews.cs.nds.rpc.JSONValue;
import uk.ac.standrews.cs.nds.rpc.Marshaller;

public class TableManagerServer extends ApplicationServer {

    private final ITableManagerRemote table_manager;
    private final HashMap<String, IHandler> handler_map;
    private final H2OMarshaller marshaller;

    public TableManagerServer(final ITableManagerRemote table_manager) {

        this.table_manager = table_manager;
        handler_map = new HashMap<String, IHandler>();

        marshaller = new H2OMarshaller();
        initHandlers();
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public Marshaller getMarshaller() {

        return marshaller;
    }

    @Override
    public IHandler getHandler(final String method_name) {

        return handler_map.get(method_name);
    }

    // -------------------------------------------------------------------------------------------------------

    private void initHandlers() {

        // public final TableProxy getTableProxy(final LockType lockType, final LockRequest lockRequest) throws RPCException, SQLException, MovedException;

        handler_map.put("getTableProxy", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final LockType p0 = marshaller.deserializeLockType(args.getString(0));
                final LockRequest p1 = marshaller.deserializeLockRequest(args.getJSONObject(1));
                return marshaller.serializeTableProxy(table_manager.getTableProxy(p0, p1));
            }
        });

        // public final boolean addTableInformation(final DatabaseID tableManagerURL, final TableInfo tableDetails) throws RPCException, MovedException, SQLException;

        handler_map.put("addTableInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final DatabaseID p0 = marshaller.deserializeDatabaseID(args.getJSONObject(0));
                final TableInfo p1 = marshaller.deserializeTableInfo(args.getJSONObject(1));
                return new JSONValue(table_manager.addTableInformation(p0, p1));
            }
        });

        // public void addReplicaInformation(final TableInfo tableDetails) throws RPCException, MovedException, SQLException;

        handler_map.put("addReplicaInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                table_manager.addReplicaInformation(p0);
                return JSONValue.NULL;
            }
        });

        //public void removeReplicaInformation(final TableInfo ti) throws RPCException, MovedException, SQLException;

        handler_map.put("removeReplicaInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                table_manager.removeReplicaInformation(p0);
                return JSONValue.NULL;
            }
        });

        // public boolean removeTableInformation() throws RPCException, SQLException, MovedException;

        handler_map.put("removeTableInformation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(table_manager.removeTableInformation());
            }
        });

        // public DatabaseID getLocation() throws RPCException, MovedException;

        handler_map.put("getLocation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeDatabaseID(table_manager.getLocation());
            }
        });

        // public void releaseLockAndUpdateReplicaState(final boolean commit, final LockRequest requestingDatabase, final Collection<CommitResult> committedQueries, final boolean asynchronousCommit) throws RPCException, MovedException, SQLException;

        handler_map.put("releaseLockAndUpdateReplicaState", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final boolean p0 = args.getBoolean(0);
                final LockRequest p1 = marshaller.deserializeLockRequest(args.getJSONObject(1));
                final Collection<CommitResult> p2 = marshaller.deserializeCollectionCommitResult(args.getJSONArray(2));
                final boolean p3 = args.getBoolean(3);
                table_manager.releaseLockAndUpdateReplicaState(p0, p1, p2, p3);
                return JSONValue.NULL;
            }
        });

        // public void remove(final boolean dropCommand) throws RPCException;

        handler_map.put("remove", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final boolean p0 = args.getBoolean(0);
                table_manager.remove(p0);
                return JSONValue.NULL;
            }
        });

        // public String getSchemaName() throws RPCException;

        handler_map.put("getSchemaName", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(table_manager.getSchemaName());
            }
        });

        // public String getTableName() throws RPCException;

        handler_map.put("getTableName", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(table_manager.getTableName());
            }
        });

        // public int getTableSet() throws RPCException;

        handler_map.put("getTableSet", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(table_manager.getTableSet());
            }
        });

        // public void buildTableManagerState(final ITableManagerRemote oldTableManager) throws RPCException, MovedException;

        handler_map.put("buildTableManagerState", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final ITableManagerRemote p0 = marshaller.deserializeITableManagerRemote(args.getString(0));
                table_manager.buildTableManagerState(p0);
                return JSONValue.NULL;
            }
        });

        // public DatabaseID getDatabaseURL() throws RPCException;

        handler_map.put("getDatabaseURL", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeDatabaseID(table_manager.getDatabaseURL());
            }
        });

        // public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RPCException, SQLException;

        handler_map.put("recreateReplicaManagerState", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final String p0 = args.getString(0);
                table_manager.recreateReplicaManagerState(p0);
                return JSONValue.NULL;

            }
        });

        // public int getNumberofReplicas() throws RPCException;

        handler_map.put("getNumberofReplicas", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(table_manager.getNumberofReplicas());
            }
        });

        // public void persistToCompleteStartup(TableInfo ti) throws RPCException, StartupException;

        handler_map.put("persistToCompleteStartup", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                table_manager.persistToCompleteStartup(p0);
                return JSONValue.NULL;
            }
        });

        // public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() throws RPCException, MovedException

        handler_map.put("getActiveReplicas", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapDatabaseInstanceWrapperInteger(table_manager.getActiveReplicas());
            }
        });

        // public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() throws RPCException, MovedException

        handler_map.put("getAllReplicas", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeMapDatabaseInstanceWrapperInteger(table_manager.getAllReplicas());
            }
        });

        // public TableInfo getTableInfo() throws RPCException

        handler_map.put("getTableInfo", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeTableInfo(table_manager.getTableInfo());
            }
        });
    }
}
