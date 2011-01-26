package org.h2o.db;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.IHandler;
import uk.ac.standrews.cs.nds.rpc.JSONValue;
import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.NetworkUtil;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class DatabaseInstanceServer extends ApplicationServer {

    private final Map<String, IHandler> handler_map;
    private final IDatabaseInstanceRemote instance;
    private static final H2OMarshaller marshaller = new H2OMarshaller();

    public DatabaseInstanceServer(final IDatabaseInstanceRemote instance, final int port) {

        super.setPort(port);
        try {
            super.setLocalAddress(NetworkUtil.getLocalIPv4Address());
        }
        catch (final UnknownHostException e) {
            ErrorHandling.hardExceptionError(e, "Couldn't find local IP address.");
        }

        this.instance = instance;
        handler_map = new HashMap<String, IHandler>();

        initHandlers();
    }

    @Override
    public Marshaller getMarshaller() {

        return marshaller;
    }

    @Override
    public IHandler getHandler(final String method_name) {

        return handler_map.get(method_name);
    }

    private void initHandlers() {

        // String getConnectionString() throws RPCException;

        handler_map.put("getConnectionString", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(instance.getConnectionString());
            }
        });

        // DatabaseID getURL() throws RPCException;

        handler_map.put("getURL", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeDatabaseID(instance.getURL());
            }
        });

        // DatabaseID getSystemTableURL() throws RPCException;

        handler_map.put("getSystemTableURL", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeDatabaseID(instance.getSystemTableURL());
            }
        });

        // final int executeUpdate(final String sql, final boolean systemTableCommand) throws RPCException, SQLException;

        handler_map.put("executeUpdate", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final String p0 = args.getString(0);
                final boolean p1 = args.getBoolean(1);
                return new JSONValue(instance.executeUpdate(p0, p1));
            }
        });

        // void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseID databaseURL) throws RPCException;

        handler_map.put("setSystemTableLocation", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final IChordRemoteReference p0 = marshaller.deserializeChordRemoteReference(args.getJSONObject(0));
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args.getJSONObject(1));
                instance.setSystemTableLocation(p0, p1);
                return JSONValue.NULL;
            }
        });

        // final ITableManagerRemote findTableManagerReference(final TableInfo tableInfo, final boolean searchOnlyCache) throws RPCException;

        handler_map.put("findTableManagerReference", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                final boolean p1 = args.getBoolean(1);
                return marshaller.serializeITableManagerRemote(instance.findTableManagerReference(p0, p1));
            }
        });

        // void setAlive(boolean alive) throws RPCException;

        handler_map.put("setAlive", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final boolean p0 = args.getBoolean(0);
                instance.setAlive(p0);
                return JSONValue.NULL;
            }
        });

        // ISystemTableRemote recreateSystemTable() throws RPCException, SQLException, SystemTableAccessException;

        handler_map.put("recreateSystemTable", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeISystemTableMigratable(instance.recreateSystemTable());
            }
        });

        // boolean recreateTableManager(TableInfo tableInfo, DatabaseID databaseURL) throws RPCException;

        handler_map.put("recreateTableManager", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args.getJSONObject(0));
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args.getJSONObject(1));
                return new JSONValue(instance.recreateTableManager(p0, p1));
            }
        });

        // boolean isSystemTable() throws RPCException;

        handler_map.put("isSystemTable", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return new JSONValue(instance.isSystemTable());
            }
        });

        // ISystemTableRemote getSystemTable() throws RPCException;

        handler_map.put("getSystemTable", new IHandler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                return marshaller.serializeISystemTableMigratable(instance.getSystemTable());
            }
        });

    }
}
