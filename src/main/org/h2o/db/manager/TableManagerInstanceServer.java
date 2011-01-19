package org.h2o.db.manager;

import java.util.HashMap;
import java.util.Map;

import org.h2o.db.interfaces.ITableManagerRemote;
import org.json.JSONArray;

import uk.ac.standrews.cs.nds.rpc.ApplicationServer;
import uk.ac.standrews.cs.nds.rpc.Handler;
import uk.ac.standrews.cs.nds.rpc.JSONValue;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * This class maintains a Map of exported Table Managers and provides an RMI mechanism over the underlying JSON RPC mechanism.
 * It assumes that the Table Manager is provided as the zeroth parameter on the wire.
 * This parameter is removed before calling the appropriate method in the appropriate TableManager.
 *
 * @author Alan Dearle (al@cs.st-andrews.ac.uk)
 */
public class TableManagerInstanceServer extends ApplicationServer {

    /**
     *  Map from fully qualified table name to table manage instance.
     */
    private final Map<String, TableManagerServer> table_manager_instances;

    public TableManagerInstanceServer() {

        table_manager_instances = new HashMap<String, TableManagerServer>();
    }

    /**
     * This method is used to expose a ITableManagerRemote so that it can be accessed remotely.
     * @param tm - a tablemanager to be made eternally addressible 
     */
    public void exportObject(final ITableManagerRemote tm) {

        try {
            table_manager_instances.put(tm.getTableInfo().getFullTableName(), new TableManagerServer(tm));
        }
        catch (final RPCException e) {
            // This should never happen because the object being exported is always local
            // Therefore hard Error.
            ErrorHandling.hardError("RPC error on (what should be) local object");
        }

    }

    /**
     * This makes an RMI call on the object identified in by the zeroth parameter from the JSON array of args
     * and on the method whose name is specified in @param method_name.
     */
    @Override
    public Handler getHandler(final String method_name) {

        return new Handler() {

            @Override
            public JSONValue execute(final JSONArray args) throws Exception {

                final String table_name = args.getString(0);
                args.remove(0); // remove the table name from the parameter list
                final TableManagerServer object_server = table_manager_instances.get(table_name);
                return object_server.getHandler(method_name).execute(args);

            }
        };
    }
}
