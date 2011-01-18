package org.h2o.db;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableRemote;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.replication.ReplicaManager;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.json.JSONObject;

import uk.ac.standrews.cs.nds.rpc.DeserializationException;
import uk.ac.standrews.cs.nds.rpc.JSONValue;
import uk.ac.standrews.cs.nds.rpc.Marshaller;
import uk.ac.standrews.cs.stachord.impl.ChordRemoteMarshaller;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class H2OMarshaller extends Marshaller {

    private final ChordRemoteMarshaller chord_marshaller;

    public H2OMarshaller() {

        chord_marshaller = new ChordRemoteMarshaller();
    }

    /////////////////////

    public JSONValue serializeChordRemoteReference(final IChordRemoteReference source) {

        return chord_marshaller.serializeChordRemoteReference(source);
    }

    public IChordRemoteReference deserializeChordRemoteReference(final JSONObject jsonObject) throws DeserializationException {

        return chord_marshaller.deserializeChordRemoteReference(jsonObject);
    }

    /////////////////////

    public JSONValue serializeDatabaseID(final DatabaseID source) {

        // TODO Auto-generated method stub
        return null;
    }

    public DatabaseID deserializeDatabaseID(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeTableInfo(final TableInfo source) {

        // TODO Auto-generated method stub
        return null;
    }

    public TableInfo deserializeTableInfo(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeITableManagerRemote(final ITableManagerRemote source) {

        // TODO Auto-generated method stub
        return null;
    }

    public ITableManagerRemote deserializeITableManagerRemote(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeISystemTableRemote(final ISystemTableRemote source) {

        // TODO Auto-generated method stub
        return null;
    }

    public ISystemTableRemote deserializeSystemTableRemote(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeLockRequest(final LockRequest source) {

        // TODO Auto-generated method stub
        return null;
    }

    public LockRequest deserializeLockRequest(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeLockType(final LockType source) {

        // TODO Auto-generated method stub
        return null;
    }

    public LockType deserializeLockType(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeReplicaManager(final ReplicaManager source) {

        // TODO Auto-generated method stub
        return null;
    }

    public ReplicaManager deserializeReplicaManager(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeSetDatabaseInstanceWrapper(final Set<DatabaseInstanceWrapper> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Set<DatabaseInstanceWrapper> deserializeSetDatabaseInstanceWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeDatabaseInstanceWrapper(final DatabaseInstanceWrapper source) {

        // TODO Auto-generated method stub
        return null;
    }

    public DatabaseInstanceWrapper deserializeDatabaseInstanceWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeISystemTable(final ISystemTable source) {

        // TODO Auto-generated method stub
        return null;
    }

    public ISystemTable deserializeISystemTable(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeMapDatabaseIDDatabaseInstanceWrapper(final Map<DatabaseID, DatabaseInstanceWrapper> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Map<DatabaseID, DatabaseInstanceWrapper> deserializeMapDatabaseIDDatabaseInstanceWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeMapTableInfoTableManagerWrapper(final Map<TableInfo, TableManagerWrapper> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Map<TableInfo, TableManagerWrapper> deserializeMapTableInfoTableManagerWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeMapTableInfoSetDatabaseID(final Map<TableInfo, Set<DatabaseID>> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Map<TableInfo, Set<DatabaseID>> deserializeMapTableInfoSetDatabaseID(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeIDatabaseInstanceRemote(final IDatabaseInstanceRemote source) {

        // TODO Auto-generated method stub
        return null;
    }

    public IDatabaseInstanceRemote deserializeIDatabaseInstanceRemote(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeTableManagerWrapper(final TableManagerWrapper source) {

        // TODO Auto-generated method stub
        return null;
    }

    public TableManagerWrapper deserializeTableManagerWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeSetTableManagerWrapper(final Set<TableManagerWrapper> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Set<TableManagerWrapper> deserializeSetTableManagerWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeMapTableInfoDatabaseID(final Map<TableInfo, DatabaseID> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Map<TableInfo, DatabaseID> deserializeMapTableInfoDatabaseID(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeTableProxy(final TableProxy source) {

        // TODO Auto-generated method stub
        return null;
    }

    public TableProxy deserializeTableProxy(final JSONValue jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeCollectionCommitResult(final Collection<CommitResult> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Collection<CommitResult> deserializeCollectionCommitResult(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeSetString(final Set<String> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Set<String> deserializeSetString(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeActionRequest(final ActionRequest source) {

        // TODO Auto-generated method stub
        return null;
    }

    public ActionRequest deserializeActionRequest(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }

    /////////////////////

    public JSONValue serializeQueueDatabaseInstanceWrapper(final Queue<DatabaseInstanceWrapper> source) {

        // TODO Auto-generated method stub
        return null;
    }

    public Queue<DatabaseInstanceWrapper> deserializeQueueDatabaseInstanceWrapper(final JSONObject jsonObject) {

        // TODO Auto-generated method stub
        return null;
    }
}
