package org.h2o.db;

import java.net.UnknownHostException;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.json.JSONWriter;

import uk.ac.standrews.cs.nds.rpc.stream.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.stream.IHandler;
import uk.ac.standrews.cs.nds.rpc.stream.JSONReader;
import uk.ac.standrews.cs.nds.rpc.stream.Marshaller;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.NetworkUtil;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class DatabaseInstanceServer extends ApplicationServer {

    private static final String DEFAULT_REGISTRY_KEY = "H2O_DATABASE";

    private final IDatabaseInstanceRemote instance;
    private static final H2OMarshaller marshaller = new H2OMarshaller();

    public DatabaseInstanceServer(final IDatabaseInstanceRemote instance, final int port) {

        this(instance, port, DEFAULT_REGISTRY_KEY);
    }

    public DatabaseInstanceServer(final IDatabaseInstanceRemote instance, final int port, final String registry_key) {

        super.setPort(port);
        try {
            super.setLocalAddress(NetworkUtil.getLocalIPv4Address());
        }
        catch (final UnknownHostException e) {
            ErrorHandling.hardExceptionError(e, "Couldn't find local IP address.");
        }

        this.instance = instance;
        this.registry_key = registry_key;

        initHandlers();
    }

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

        // String getConnectionString() throws RPCException;

        handler_map.put("getConnectionString", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(instance.getConnectionString());
            }
        });

        // DatabaseID getURL() throws RPCException;

        handler_map.put("getURL", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeDatabaseID(instance.getURL(), response);
            }
        });

        handler_map.put("isAlive", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(instance.isAlive());
            }
        });

        // DatabaseID getSystemTableURL() throws RPCException;

        handler_map.put("getSystemTableURL", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeDatabaseID(instance.getSystemTableURL(), response);
            }
        });

        // final int executeUpdate(final String sql, final boolean systemTableCommand) throws RPCException, SQLException;

        handler_map.put("executeUpdate", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final String p0 = args.stringValue();
                final boolean p1 = args.booleanValue();
                response.value(instance.executeUpdate(p0, p1));
            }
        });

        // void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseID databaseURL) throws RPCException;

        handler_map.put("setSystemTableLocation", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final IChordRemoteReference p0 = marshaller.deserializeChordRemoteReference(args);
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args);
                instance.setSystemTableLocation(p0, p1);
                response.value("");
            }
        });

        // final ITableManagerRemote findTableManagerReference(final TableInfo tableInfo, final boolean searchOnlyCache) throws RPCException;

        handler_map.put("findTableManagerReference", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                final boolean p1 = args.booleanValue();
                marshaller.serializeITableManagerRemote(instance.findTableManagerReference(p0, p1), response);
            }
        });

        // void setAlive(boolean alive) throws RPCException;

        handler_map.put("setAlive", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final boolean p0 = args.booleanValue();
                instance.setAlive(p0);
                response.value("");
            }
        });

        // ISystemTableRemote recreateSystemTable() throws RPCException, SQLException, SystemTableAccessException;

        handler_map.put("recreateSystemTable", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeISystemTableMigratable(instance.recreateSystemTable(), response);
            }
        });

        // boolean recreateTableManager(TableInfo tableInfo, DatabaseID databaseURL) throws RPCException;

        handler_map.put("recreateTableManager", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final TableInfo p0 = marshaller.deserializeTableInfo(args);
                final DatabaseID p1 = marshaller.deserializeDatabaseID(args);
                response.value(instance.recreateTableManager(p0, p1));
            }
        });

        // boolean isSystemTable() throws RPCException;

        handler_map.put("isSystemTable", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(instance.isSystemTable());
            }
        });

        // ISystemTableRemote getSystemTable() throws RPCException;

        handler_map.put("getSystemTable", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final ISystemTableMigratable systemTable = instance.getSystemTable();
                if (systemTable != null) {
                    marshaller.serializeISystemTableMigratable(systemTable, response);
                }
                else {
                    response.value(null);
                }
            }
        });

        handler_map.put("getAddress", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                marshaller.serializeInetSocketAddress(instance.getAddress(), response);
            }
        });

        //int execute(String query, String transactionName, boolean commitOperation) throws RPCException, SQLException;

        handler_map.put("execute", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                final String p0 = args.stringValue();
                final String p1 = args.stringValue();
                final boolean p2 = args.booleanValue();
                response.value(instance.execute(p0, p1, p2));
            }
        });

        handler_map.put("getChordPort", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(instance.getChordPort());
            }
        });

        handler_map.put("isReplicating", new IHandler() {

            @Override
            public void execute(final JSONReader args, final JSONWriter response) throws Exception {

                response.value(instance.isReplicating());
            }
        });
    }
}
