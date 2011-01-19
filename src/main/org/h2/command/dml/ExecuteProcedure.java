/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Procedure;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * This class represents the statement EXECUTE
 */
public class ExecuteProcedure extends Prepared {

    private final ObjectArray expressions = new ObjectArray();

    private Procedure procedure;

    public ExecuteProcedure(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    public void setProcedure(final Procedure procedure) {

        this.procedure = procedure;
    }

    /**
     * Set the expression at the given index.
     * 
     * @param index
     *            the index (0 based)
     * @param expr
     *            the expression
     */
    public void setExpression(final int index, final Expression expr) {

        expressions.add(index, expr);
    }

    private void setParameters() throws SQLException {

        final Prepared prepared = procedure.getPrepared();
        final ObjectArray params = prepared.getParameters();
        for (int i = 0; params != null && i < params.size() && i < expressions.size(); i++) {
            final Expression expr = (Expression) expressions.get(i);
            final Parameter p = (Parameter) params.get(i);
            p.setValue(expr.getValue(session));
        }
    }

    @Override
    public boolean isQuery() {

        final Prepared prepared = procedure.getPrepared();
        return prepared.isQuery();
    }

    @Override
    public int update() throws SQLException, RPCException {

        setParameters();
        final Prepared prepared = procedure.getPrepared();
        return prepared.update();
    }

    @Override
    public final LocalResult query(final int limit) throws SQLException {

        setParameters();
        final Prepared prepared = procedure.getPrepared();
        return prepared.query(limit);
    }

    @Override
    public boolean isTransactional() {

        return true;
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        final Prepared prepared = procedure.getPrepared();
        return prepared.queryMeta();
    }

}
