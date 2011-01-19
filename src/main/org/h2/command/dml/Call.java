/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueResultSet;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * This class represents the statement CALL.
 */
public class Call extends Prepared {

    private Expression value;

    private ObjectArray expressions;

    public Call(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        final LocalResult result = new LocalResult(session, expressions, 1);
        result.done();
        return result;
    }

    @Override
    public int update() throws SQLException, RPCException {

        final Value v = value.getValue(session);
        final int type = v.getType();
        switch (type) {
            case Value.RESULT_SET:
            case Value.ARRAY:
                // this will throw an exception
                // methods returning a result set may not be called like this.
                return super.update();
            case Value.UNKNOWN:
            case Value.NULL:
                return 0;
            default:
                return v.getInt();
        }
    }

    @Override
    public LocalResult query(final int maxrows) throws SQLException {

        setCurrentRowNumber(1);
        final Value v = value.getValue(session);
        if (v.getType() == Value.RESULT_SET) {
            final ResultSet rs = ((ValueResultSet) v).getResultSet();
            return LocalResult.read(session, rs, maxrows);
        }
        else if (v.getType() == Value.ARRAY) {
            final Value[] list = ((ValueArray) v).getList();
            final ObjectArray expr = new ObjectArray();
            for (int i = 0; i < list.length; i++) {
                final Value e = list[i];
                final Column col = new Column("C" + (i + 1), e.getType(), e.getPrecision(), e.getScale(), e.getDisplaySize());
                expr.add(new ExpressionColumn(session.getDatabase(), col));
            }
            final LocalResult result = new LocalResult(session, expr, list.length);
            result.addRow(list);
            result.done();
            return result;
        }
        final LocalResult result = new LocalResult(session, expressions, 1);
        final Value[] row = new Value[1];
        row[0] = v;
        result.addRow(row);
        result.done();
        return result;
    }

    @Override
    public void prepare() throws SQLException {

        value = value.optimize(session);
        expressions = new ObjectArray();
        expressions.add(value);
    }

    public void setValue(final Expression expression) {

        value = expression;
    }

    @Override
    public boolean isQuery() {

        return true;
    }

    @Override
    public boolean isTransactional() {

        return true;
    }

    @Override
    public boolean isReadOnly() {

        return value.isEverything(ExpressionVisitor.READONLY);

    }

}
