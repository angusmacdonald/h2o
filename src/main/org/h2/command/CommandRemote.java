/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.ResultRemote;
import org.h2.util.ObjectArray;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * Represents the client-side part of a SQL statement. This class is not used in embedded mode.
 */
public class CommandRemote implements CommandInterface {

    private final ObjectArray transferList;

    private final ObjectArray parameters;

    private final Trace trace;

    private final String sql;

    private final int fetchSize;

    private SessionRemote session;

    private int id;

    private boolean isQuery;

    private boolean readonly;

    private int paramCount;

    private final int created;

    public CommandRemote(final SessionRemote session, final ObjectArray transferList, final String sql, final int fetchSize) throws SQLException {

        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = new ObjectArray();
        prepare(session, true);
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
        this.fetchSize = fetchSize;
        created = session.getLastReconnect();
    }

    private void prepare(final SessionRemote s, final boolean createParams) throws SQLException {

        id = s.getNextId();
        paramCount = 0;
        final boolean readParams = s.getClientVersion() >= Constants.TCP_PROTOCOL_VERSION_6;
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                final Transfer transfer = (Transfer) transferList.get(i);
                if (readParams && createParams) {
                    s.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE_READ_PARAMS).writeInt(id).writeString(sql);
                }
                else {
                    s.traceOperation("SESSION_PREPARE", id);
                    transfer.writeInt(SessionRemote.SESSION_PREPARE).writeInt(id).writeString(sql);
                }
                s.done(transfer);
                isQuery = transfer.readBoolean();
                readonly = transfer.readBoolean();
                paramCount = transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        if (readParams) {
                            final ParameterRemote p = new ParameterRemote(j);
                            p.readMetaData(transfer);
                            parameters.add(p);
                        }
                        else {
                            parameters.add(new ParameterRemote(j));
                        }
                    }
                }
            }
            catch (final IOException e) {
                // Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Error contacting server. It will be removed from the active set.");
                s.removeServer(e, i--, ++count);

            }
        }
    }

    @Override
    public boolean isQuery() {

        return isQuery;
    }

    @Override
    public ObjectArray getParameters() {

        return parameters;
    }

    private void prepareIfRequired() throws SQLException {

        if (session.getLastReconnect() != created) {
            // in this case we need to prepare again in every case
            id = Integer.MIN_VALUE;
        }
        session.checkClosed();
        if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(session, false);
        }
    }

    @Override
    public ResultInterface getMetaData() throws SQLException {

        synchronized (session) {
            if (!isQuery) { return null; }
            final int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                final Transfer transfer = (Transfer) transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each
                    // server / auto detect
                    session.traceOperation("COMMAND_GET_META_DATA", id);
                    transfer.writeInt(SessionRemote.COMMAND_GET_META_DATA).writeInt(id).writeInt(objectId);
                    session.done(transfer);
                    final int columnCount = transfer.readInt();
                    result = new ResultRemote(session, transfer, objectId, columnCount, Integer.MAX_VALUE);
                    break;
                }
                catch (final IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }
    }

    @Override
    public ResultInterface executeQuery(final int maxRows, final boolean scrollable) throws SQLException {

        checkParameters();
        synchronized (session) {
            final int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                final Transfer transfer = (Transfer) transferList.get(i);
                try {
                    // TODO cluster: support load balance with values for each
                    // server / auto detect
                    session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_QUERY).writeInt(id).writeInt(objectId).writeInt(maxRows);
                    int fetch;
                    if (session.isClustered() || scrollable) {
                        fetch = Integer.MAX_VALUE;
                    }
                    else {
                        fetch = fetchSize;
                    }
                    transfer.writeInt(fetch);
                    sendParameters(transfer);
                    session.done(transfer);
                    final int columnCount = transfer.readInt();
                    if (result != null) {
                        result.close();
                        result = null;
                    }
                    result = new ResultRemote(session, transfer, objectId, columnCount, fetch);
                    if (readonly) {
                        break;
                    }
                }
                catch (final IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            session.readSessionState();
            return result;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {

        checkParameters();
        synchronized (session) {
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                final Transfer transfer = (Transfer) transferList.get(i);
                try {
                    session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_UPDATE).writeInt(id);
                    sendParameters(transfer);
                    session.done(transfer);
                    updateCount = transfer.readInt();
                    autoCommit = transfer.readBoolean();
                }
                catch (final IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.setAutoCommit(autoCommit);
            session.autoCommitIfCluster();
            session.readSessionState();
            return updateCount;
        }
    }

    private void checkParameters() throws SQLException {

        final int len = parameters.size();
        for (int i = 0; i < len; i++) {
            final ParameterInterface p = (ParameterInterface) parameters.get(i);
            p.checkSet();
        }
    }

    private void sendParameters(final Transfer transfer) throws IOException, SQLException {

        final int len = parameters.size();
        transfer.writeInt(len);
        for (int i = 0; i < len; i++) {
            final ParameterInterface p = (ParameterInterface) parameters.get(i);
            transfer.writeValue(p.getParamValue());
        }
    }

    @Override
    public void close() {

        if (session == null || session.isClosed()) { return; }
        synchronized (session) {
            for (int i = 0; i < transferList.size(); i++) {
                try {
                    final Transfer transfer = (Transfer) transferList.get(i);
                    session.traceOperation("COMMAND_CLOSE", id);
                    transfer.writeInt(SessionRemote.COMMAND_CLOSE).writeInt(id);
                }
                catch (final IOException e) {
                    trace.error("close", e);
                }
            }
        }
        session = null;
        final int len = parameters.size();
        try {
            for (int i = 0; i < len; i++) {
                final ParameterInterface p = (ParameterInterface) parameters.get(i);
                final Value v = p.getParamValue();
                if (v != null) {
                    v.close();
                }
            }
        }
        catch (final SQLException e) {
            trace.error("close", e);
        }
        parameters.clear();
    }

    /**
     * Cancel this current statement.
     */
    @Override
    public void cancel() {

        session.cancelStatement(id);
    }

    @Override
    public String toString() {

        return TraceObject.toString(sql, getParameters());
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.CommandInterface#executeUpdate(boolean)
     */
    @Override
    public int executeUpdate(final boolean isMultiQueryTransaction) throws SQLException {

        return executeUpdate();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.CommandInterface#isPreparedStatement(boolean)
     */
    @Override
    public void setIsPreparedStatement(final boolean preparedStatement) {

        // ErrorHandling.hardError("Didn't expect this to be called.");
    }

}
