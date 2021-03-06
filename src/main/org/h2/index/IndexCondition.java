/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Message;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * A index condition object is made for each condition that can potentially use an index. This class does not extend expression, but in
 * general there is one expression that maps to each index condition.
 */
public class IndexCondition {

    /**
     * A bit of a search mask meaning 'equal'.
     */
    public static final int EQUALITY = 1;

    /**
     * A bit of a search mask meaning 'larger or equal'.
     */
    public static final int START = 2;

    /**
     * A bit of a search mask meaning 'smaller or equal'.
     */
    public static final int END = 4;

    /**
     * A search mask meaning 'between'.
     */
    public static final int RANGE = START | END;

    /**
     * A bit of a search mask meaning 'the condition is always false'.
     */
    public static final int ALWAYS_FALSE = 8;

    private Column column;

    private Expression expression;

    private int compareType;

    /**
     * Create an index condition with the given parameters.
     * 
     * @param compareType
     *            the comparison type
     * @param column
     *            the column
     * @param expression
     *            the expression
     */
    public IndexCondition(int compareType, ExpressionColumn column, Expression expression) {

        this.compareType = compareType;
        this.column = column == null ? null : column.getColumn();
        this.expression = expression;
    }

    /**
     * Get the current value of the expression.
     * 
     * @param session
     *            the session
     * @return the value
     */
    public Value getCurrentValue(Session session) throws SQLException {

        return expression.getValue(session);
    }

    /**
     * Get the SQL snippet of this comparison.
     * 
     * @return the SQL snippet
     */
    public String getSQL() {

        if (compareType == Comparison.FALSE) { return "FALSE"; }
        StringBuilder buff = new StringBuilder();
        buff.append(column.getSQL());
        switch (compareType) {
            case Comparison.EQUAL:
                buff.append(" = ");
                break;
            case Comparison.BIGGER_EQUAL:
                buff.append(" >= ");
                break;
            case Comparison.BIGGER:
                buff.append(" > ");
                break;
            case Comparison.SMALLER_EQUAL:
                buff.append(" <= ");
                break;
            case Comparison.SMALLER:
                buff.append(" < ");
                break;
            default:
                Message.throwInternalError("type=" + compareType);
        }
        buff.append(expression.getSQL());
        return buff.toString();
    }

    /**
     * Get the comparison bit mask.
     * 
     * @return the mask
     */
    public int getMask() {

        switch (compareType) {
            case Comparison.FALSE:
                return ALWAYS_FALSE;
            case Comparison.EQUAL:
                return EQUALITY;
            case Comparison.BIGGER_EQUAL:
            case Comparison.BIGGER:
                return START;
            case Comparison.SMALLER_EQUAL:
            case Comparison.SMALLER:
                return END;
            default:
                throw Message.throwInternalError("type=" + compareType);
        }
    }

    /**
     * Check if the result is always false.
     * 
     * @return true if the result will always be false
     */
    public boolean isAlwaysFalse() {

        return compareType == Comparison.FALSE;
    }

    /**
     * Check if this index condition is of the type column larger or equal to value.
     * 
     * @return true if this is a start condition
     */
    public boolean isStart() {

        switch (compareType) {
            case Comparison.EQUAL:
            case Comparison.BIGGER_EQUAL:
            case Comparison.BIGGER:
                return true;
            default:
                return false;
        }
    }

    /**
     * Check if this index condition is of the type column smaller or equal to value.
     * 
     * @return true if this is a end condition
     */
    public boolean isEnd() {

        switch (compareType) {
            case Comparison.EQUAL:
            case Comparison.SMALLER_EQUAL:
            case Comparison.SMALLER:
                return true;
            default:
                return false;
        }
    }

    /**
     * Get the referenced column.
     * 
     * @return the column
     */
    public Column getColumn() {

        return column;
    }

    /**
     * Check if the expression can be evaluated.
     * 
     * @return true if it can be evaluated
     */
    public boolean isEvaluatable() {

        return expression.isEverything(ExpressionVisitor.EVALUATABLE);
    }

}
