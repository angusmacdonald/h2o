/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.Collator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.h2.api.Trigger;
import org.h2.command.ddl.AlterIndexRename;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.AlterTableAlterColumn;
import org.h2.command.ddl.AlterTableDropConstraint;
import org.h2.command.ddl.AlterTableRename;
import org.h2.command.ddl.AlterTableRenameColumn;
import org.h2.command.ddl.AlterUser;
import org.h2.command.ddl.AlterView;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CreateAggregate;
import org.h2.command.ddl.CreateConstant;
import org.h2.command.ddl.CreateFunctionAlias;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateLinkedTable;
import org.h2.command.ddl.CreateRole;
import org.h2.command.ddl.CreateSchema;
import org.h2.command.ddl.CreateSequence;
import org.h2.command.ddl.CreateTable;
import org.h2.command.ddl.CreateTrigger;
import org.h2.command.ddl.CreateUser;
import org.h2.command.ddl.CreateUserDataType;
import org.h2.command.ddl.CreateView;
import org.h2.command.ddl.DeallocateProcedure;
import org.h2.command.ddl.DropAggregate;
import org.h2.command.ddl.DropConstant;
import org.h2.command.ddl.DropDatabase;
import org.h2.command.ddl.DropFunctionAlias;
import org.h2.command.ddl.DropIndex;
import org.h2.command.ddl.DropRole;
import org.h2.command.ddl.DropSchema;
import org.h2.command.ddl.DropSequence;
import org.h2.command.ddl.DropTable;
import org.h2.command.ddl.DropTrigger;
import org.h2.command.ddl.DropUser;
import org.h2.command.ddl.DropUserDataType;
import org.h2.command.ddl.DropView;
import org.h2.command.ddl.GrantRevoke;
import org.h2.command.ddl.PrepareProcedure;
import org.h2.command.ddl.SetComment;
import org.h2.command.ddl.TruncateTable;
import org.h2.command.dml.AlterSequence;
import org.h2.command.dml.AlterTableSet;
import org.h2.command.dml.BackupCommand;
import org.h2.command.dml.Call;
import org.h2.command.dml.Delete;
import org.h2.command.dml.ExecuteProcedure;
import org.h2.command.dml.ExplainPlan;
import org.h2.command.dml.GetRmiPort;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.NoOperation;
import org.h2.command.dml.Query;
import org.h2.command.dml.RunScriptCommand;
import org.h2.command.dml.ScriptCommand;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.command.dml.SelectUnion;
import org.h2.command.dml.Set;
import org.h2.command.dml.SetTypes;
import org.h2.command.dml.TransactionCommand;
import org.h2.command.dml.Update;
import org.h2.command.h2o.CreateReplica;
import org.h2.command.h2o.DropReplica;
import org.h2.command.h2o.GetMetaDatReplicationFactor;
import org.h2.command.h2o.GetReplicationFactor;
import org.h2.command.h2o.MigrateSystemTable;
import org.h2.command.h2o.MigrateTableManager;
import org.h2.command.h2o.RecreateTableManager;
import org.h2.command.h2o.SetReplicate;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constant.SysProperties;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Procedure;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.Aggregate;
import org.h2.expression.Alias;
import org.h2.expression.CompareLike;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.ConditionExists;
import org.h2.expression.ConditionIn;
import org.h2.expression.ConditionInSelect;
import org.h2.expression.ConditionNot;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.Function;
import org.h2.expression.FunctionCall;
import org.h2.expression.JavaAggregate;
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.Rownum;
import org.h2.expression.SequenceValue;
import org.h2.expression.Subquery;
import org.h2.expression.TableFunction;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.expression.Wildcard;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableFilter;
import org.h2.table.TableLink;
import org.h2.table.TableView;
import org.h2.util.ByteUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringCache;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.manager.TableManagerProxy;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * The parser is used to convert a SQL statement string to an command object.
 */
public class Parser {

    // used during the tokenizer phase
    private static final int CHAR_END = -1, CHAR_VALUE = 2, CHAR_QUOTED = 3;

    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;

    private static final int CHAR_STRING = 7, CHAR_DECIMAL = 8, CHAR_DOLLAR_QUOTED_STRING = 9;

    // this are token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, PARAMETER = 3, END = 4, VALUE = 5;

    private static final int EQUAL = 6, BIGGER_EQUAL = 7, BIGGER = 8;

    private static final int SMALLER = 9, SMALLER_EQUAL = 10, NOT_EQUAL = 11, AT = 12;

    private static final int MINUS = 17, PLUS = 18;

    private static final int STRING_CONCAT = 22;

    private static final int OPEN = 31, CLOSE = 32, NULL = 34, TRUE = 40, FALSE = 41;

    private static final int CURRENT_TIMESTAMP = 42, CURRENT_DATE = 43, CURRENT_TIME = 44, ROWNUM = 45;

    private final Database database;

    private final Session session;

    private int[] characterTypes;

    private int currentTokenType;

    private String currentToken;

    private boolean currentTokenQuoted;

    private Value currentValue;

    private String sqlCommand;

    private String originalSQL;

    private char[] sqlCommandChars;

    private int lastParseIndex;

    private int parseIndex;

    private Prepared prepared;

    private Prepared currentPrepared;

    private Select currentSelect;

    private ObjectArray parameters;

    private String schemaName;

    private ObjectArray expectedList;

    private boolean rightsChecked;

    private boolean recompileAlways;

    private ObjectArray indexedParameterList;

    /**
     * True if this query has been sent internally through the RMI interface, false if it has come from an external JBDC connection.
     */
    private final boolean internalQuery;

    /**
     * 
     * @param session
     *            The current session.
     * @param internalQuery
     *            True if this query has been sent internally through the RMI interface, false if it has come from an external JBDC
     *            connection.
     */
    public Parser(final Session session, final boolean internalQuery) {

        database = session.getDatabase();
        this.session = session;
        this.internalQuery = internalQuery;
    }

    /**
     * Parse the statement and prepare it for execution.
     * 
     * @param sql
     *            the SQL statement to parse
     * @return the prepared object
     */
    public Prepared prepare(final String sql) throws SQLException {

        try {
            final Prepared p = parse(sql);
            p.prepare();
            return p;
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * Parse the statement, but don't prepare it for execution.
     * 
     * @param sql
     *            the SQL statement to parse
     * @return the prepared object
     */
    public Prepared parseOnly(final String sql) throws SQLException {

        try {
            return parse(sql);
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * Parse a statement or a list of statements, and prepare it for execution.
     * 
     * @param sql
     *            the SQL statement to parse
     * @return the command object
     */
    public Command prepareCommand(final String sql) throws SQLException {

        try {
            final Prepared p = parse(sql);
            p.prepare();

            Command c = new CommandContainer(this, sql, p);

            p.setCommand(c);
            if (isToken(";")) {
                final String remaining = originalSQL.substring(parseIndex);
                if (remaining.trim().length() != 0) {
                    final Command list = new CommandList(this, sql, c, remaining);
                    // list.addCommand(c);
                    // do {
                    // c = parseCommand();
                    // list.addCommand(c);
                    // } while(currentToken.equals(";"));
                    c = list;
                }
            }
            else if (currentTokenType != END) { throw getSyntaxError(); }
            return c;
        }
        catch (final Exception e) {
            throw Message.addSQL(Message.convert(e), originalSQL);
        }
    }

    private Prepared parse(final String sql) throws SQLException {

        Prepared p;
        try {
            // first, try the fast variant
            p = parse(sql, false);
        }
        catch (final SQLException e) {
            if (e.getErrorCode() == ErrorCode.SYNTAX_ERROR_1) {
                // now, get the detailed exception
                p = parse(sql, true);
            }
            else {
                throw Message.addSQL(e, sql);
            }
        }
        p.setPrepareAlways(recompileAlways);
        p.setParameterList(parameters);
        return p;
    }

    private Prepared parse(final String sql, final boolean withExpectedList) throws SQLException {

        initialize(sql);
        if (withExpectedList) {
            expectedList = new ObjectArray();
        }
        else {
            expectedList = null;
        }
        parameters = new ObjectArray();
        currentSelect = null;
        currentPrepared = null;
        prepared = null;
        recompileAlways = false;
        indexedParameterList = null;
        read();
        return parsePrepared();
    }

    private Prepared parsePrepared() throws SQLException {

        final int start = lastParseIndex;
        Prepared c = null;
        final String token = currentToken;
        if (token.length() == 0) {
            c = new NoOperation(session, internalQuery);
        }
        else {
            final char first = token.charAt(0);
            switch (first) {
                case '(':
                    c = parseSelect();
                    break;
                case 'A':
                    if (readIf("ALTER")) {
                        c = parseAlter();
                    }
                    else if (readIf("ANALYZE")) {
                        c = parseAnalyze();
                    }
                    break;
                case 'B':
                    if (readIf("BACKUP")) {
                        c = parseBackup();
                    }
                    else if (readIf("BEGIN")) {
                        c = parseBegin();
                    }
                    break;
                case 'C':
                    if (readIf("COMMIT")) {
                        c = parseCommit();
                    }
                    else if (readIf("CREATE")) {
                        c = parseCreate();
                    }
                    else if (readIf("CALL")) {
                        c = parserCall();
                    }
                    else if (readIf("CHECKPOINT")) {
                        c = parseCheckpoint();
                    }
                    else if (readIf("COMMENT")) {
                        c = parseComment();
                    }
                    break;
                case 'D':
                    if (readIf("DELETE")) {
                        c = parseDelete();
                    }
                    else if (readIf("DROP")) {
                        c = parseDrop();
                    }
                    else if (readIf("DECLARE")) {
                        // support for DECLARE GLOBAL TEMPORARY TABLE...
                        c = parseCreate();
                    }
                    else if (readIf("DEALLOCATE")) {
                        c = parseDeallocate();
                    }
                    break;
                case 'E':
                    if (readIf("EXPLAIN")) {
                        c = parseExplain();
                    }
                    else if (readIf("EXECUTE")) {
                        c = parseExecute();
                    }
                    break;
                case 'F':
                    if (isToken("FROM")) {
                        c = parseSelect();
                    }
                    break;
                case 'G':
                    if (readIf("GRANT")) {
                        c = parseGrantRevoke(GrantRevoke.GRANT);
                    }
                    else if (readIf("GET")) {
                        c = parseGet();
                    }
                    break;
                case 'H':
                    if (readIf("HELP")) {
                        c = parseHelp();
                    }
                    break;
                case 'I':
                    if (readIf("INSERT")) {
                        c = parseInsert();
                    }
                    break;
                case 'M':
                    if (readIf("MERGE")) {
                        c = parseMerge();
                    }
                    else if (readIf("MIGRATE")) {
                        c = parseMigrate();
                    }
                    break;
                case 'N':
                    if (readIf("NEW")) {
                        c = parseNew();
                    }
                    break;
                case 'P':
                    if (readIf("PREPARE")) {
                        c = parsePrepare();
                    }
                    break;
                case 'R':
                    if (readIf("ROLLBACK")) {
                        c = parseRollback();
                    }
                    else if (readIf("REVOKE")) {
                        c = parseGrantRevoke(GrantRevoke.REVOKE);
                    }
                    else if (readIf("RUNSCRIPT")) {
                        c = parseRunScript();
                    }
                    else if (readIf("RELEASE")) {
                        c = parseReleaseSavepoint();
                    }
                    else if (readIf("RECREATE")) {
                        c = parseRecreate();
                    }
                    break;
                case 'S':
                    if (isToken("SELECT")) {
                        c = parseSelect();
                    }
                    else if (readIf("SET")) {
                        c = parseSet();
                    }
                    else if (readIf("SAVEPOINT")) {
                        c = parseSavepoint();
                    }
                    else if (readIf("SCRIPT")) {
                        c = parseScript();
                    }
                    else if (readIf("SHUTDOWN")) {
                        c = parseShutdown();
                    }
                    else if (readIf("SHOW")) {
                        c = parseShow();
                    }
                    break;
                case 'T':
                    if (readIf("TRUNCATE")) {
                        c = parseTruncate();
                    }
                    break;
                case 'U':
                    if (readIf("UPDATE")) {
                        c = parseUpdate();
                    }
                    break;
                case 'V':
                    if (readIf("VALUES")) {
                        c = parserCall();
                    }
                    else if (readIf("VACUUM")) {
                        c = parserVacuum();
                    }
                    break;
                case 'W':
                    if (readIf("WITH")) {
                        c = parserWith();
                    }
                    break;
                default:
                    throw getSyntaxError();
            }
            if (indexedParameterList != null) {
                for (int i = 0; i < indexedParameterList.size(); i++) {
                    if (indexedParameterList.get(i) == null) {
                        indexedParameterList.set(i, new Parameter(i));
                    }
                }
                parameters = indexedParameterList;
            }
            if (readIf("{")) {
                do {
                    final int index = (int) readLong() - 1;
                    if (index < 0 || index >= parameters.size()) { throw getSyntaxError(); }
                    final Parameter p = (Parameter) parameters.get(index);
                    if (p == null) { throw getSyntaxError(); }
                    read(":");
                    Expression expr = readExpression();
                    expr = expr.optimize(session);
                    p.setValue(expr.getValue(session));
                }
                while (readIf(","));
                read("}");
                final int len = parameters.size();
                for (int i = 0; i < len; i++) {
                    final Parameter p = (Parameter) parameters.get(i);
                    p.checkSet();
                }
                parameters.clear();
            }
        }
        if (c == null) { throw getSyntaxError(); }
        setSQL(c, null, start);
        return c;
    }

    private SQLException getSyntaxError() {

        if (expectedList == null || expectedList.size() == 0) { return Message.getSyntaxError(sqlCommand, parseIndex); }
        final StringBuilder buff = new StringBuilder();
        for (int i = 0; i < expectedList.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(expectedList.get(i));
        }
        return Message.getSyntaxError(sqlCommand, parseIndex, buff.toString());
    }

    private Prepared parseBackup() throws SQLException {

        final BackupCommand command = new BackupCommand(session, internalQuery);
        read("TO");
        command.setFileName(readExpression());
        return command;
    }

    /**
     * H2O: The VACUUM command isn't implemented in H2O, but it is used in some benchmarking tools, so its been added here. ANALYZE is
     * implemented, so if VACUUM ANALYZE is called the database will execute the latter part of the query.
     * 
     * @return
     * @throws SQLException
     */
    private Prepared parserVacuum() throws SQLException {

        if (readIf("ANALYZE")) {
            return parseAnalyze();
        }
        else {
            throw new SQLException("Not implemented in H2O.");
        }
    }

    private Prepared parseAnalyze() throws SQLException {

        final Analyze command = new Analyze(session);
        if (readIf("SAMPLE_SIZE")) {
            command.setTop(getPositiveInt());
        }
        return command;
    }

    private TransactionCommand parseBegin() throws SQLException {

        TransactionCommand command;
        if (!readIf("WORK")) {
            readIf("TRANSACTION");
        }
        command = new TransactionCommand(session, TransactionCommand.BEGIN, internalQuery);
        return command;
    }

    private TransactionCommand parseCommit() throws SQLException {

        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, TransactionCommand.COMMIT_TRANSACTION, internalQuery);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        command = new TransactionCommand(session, TransactionCommand.COMMIT, internalQuery);
        readIf("WORK");
        return command;
    }

    private TransactionCommand parseShutdown() throws SQLException {

        int type = TransactionCommand.SHUTDOWN;
        if (readIf("IMMEDIATELY")) {
            type = TransactionCommand.SHUTDOWN_IMMEDIATELY;
        }
        else {
            if (!readIf("COMPACT")) {
                readIf("SCRIPT");
            }
        }
        return new TransactionCommand(session, type, internalQuery);
    }

    private TransactionCommand parseRollback() throws SQLException {

        TransactionCommand command;
        if (readIf("TRANSACTION")) {
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TRANSACTION, internalQuery);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        if (readIf("TO")) {
            read("SAVEPOINT");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK_TO_SAVEPOINT, internalQuery);
            command.setSavepointName(readUniqueIdentifier());
        }
        else {
            readIf("WORK");
            command = new TransactionCommand(session, TransactionCommand.ROLLBACK, internalQuery);
        }
        return command;
    }

    private Prepared parsePrepare() throws SQLException {

        if (readIf("COMMIT")) {
            final TransactionCommand command = new TransactionCommand(session, TransactionCommand.PREPARE_COMMIT, internalQuery);
            command.setTransactionName(readUniqueIdentifier());
            return command;
        }
        final String procedureName = readAliasIdentifier();
        if (readIf("(")) {
            final ObjectArray list = new ObjectArray();
            for (int i = 0;; i++) {
                final Column column = parseColumnForTable("C" + i);
                list.add(column);
                if (readIf(")")) {
                    break;
                }
                read(",");
            }
        }
        read("AS");
        final Prepared prep = parsePrepared();
        final PrepareProcedure command = new PrepareProcedure(session);
        command.setProcedureName(procedureName);
        command.setPrepared(prep);
        return command;
    }

    private TransactionCommand parseSavepoint() throws SQLException {

        final TransactionCommand command = new TransactionCommand(session, TransactionCommand.SAVEPOINT, internalQuery);
        command.setSavepointName(readUniqueIdentifier());
        return command;
    }

    private Prepared parseReleaseSavepoint() throws SQLException {

        final Prepared command = new NoOperation(session, internalQuery);
        readIf("SAVEPOINT");
        readUniqueIdentifier();
        return command;
    }

    private Schema getSchema() throws SQLException {

        if (schemaName == null) { return null; }
        Schema schema = database.findSchema(schemaName);
        if (schema == null) {
            if ("SESSION".equals(schemaName)) {
                // for local temporary tables
                schema = database.getSchema(session.getCurrentSchemaName());
            }
            else {
                throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
            }
        }
        return schema;
    }

    private Column readTableColumn(final TableFilter filter) throws SQLException {

        String tableAlias = null;
        String columnName = readColumnIdentifier();
        if (readIf(".")) {
            tableAlias = columnName;
            columnName = readColumnIdentifier();
            if (readIf(".")) {
                String schema = tableAlias;
                tableAlias = columnName;
                columnName = readColumnIdentifier();
                if (readIf(".")) {
                    final String catalogName = schema;
                    schema = tableAlias;
                    tableAlias = columnName;
                    columnName = readColumnIdentifier();
                    if (!catalogName.equals(database.getShortName())) { throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, catalogName); }
                }
                if (!schema.equals(filter.getTable().getSchema().getName())) { throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schema); }
            }
            if (!tableAlias.equals(filter.getTableAlias())) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias); }
        }
        return filter.getTable().getColumn(columnName);
    }

    private Update parseUpdate() throws SQLException {

        final Update command = new Update(session, internalQuery);
        currentPrepared = command;
        final int start = lastParseIndex;
        final TableFilter filter = readSimpleTableFilter();
        command.setTableFilter(filter);
        read("SET");
        if (readIf("(")) {
            final ObjectArray columns = new ObjectArray();
            do {
                final Column column = readTableColumn(filter);
                columns.add(column);
            }
            while (readIf(","));
            read(")");
            read("=");
            final Expression expression = readExpression();
            for (int i = 0; i < columns.size(); i++) {
                final Column column = (Column) columns.get(i);
                final Function f = Function.getFunction(database, "ARRAY_GET");
                f.setParameter(0, expression);
                f.setParameter(1, ValueExpression.get(ValueInt.get(i + 1)));
                f.doneWithParameters();
                command.setAssignment(column, f);
            }
        }
        else {
            do {
                final Column column = readTableColumn(filter);
                read("=");
                Expression expression;
                if (readIf("DEFAULT")) {
                    expression = ValueExpression.getDefault();
                }
                else {
                    expression = readExpression();
                }
                command.setAssignment(column, expression);
            }
            while (readIf(","));
        }
        if (readIf("WHERE")) {
            final Expression condition = readExpression();
            command.setCondition(condition);
        }
        setSQL(command, "UPDATE", start);
        return command;
    }

    private TableFilter readSimpleTableFilter() throws SQLException {

        final Table table = readTableOrView();
        String alias = null;
        if (readIf("AS")) {
            alias = readAliasIdentifier();
        }
        else if (currentTokenType == IDENTIFIER) {
            if (!"SET".equals(currentToken)) {
                // SET is not a keyword (PostgreSQL supports it as a table name)
                alias = readAliasIdentifier();
            }
        }
        return new TableFilter(session, table, alias, rightsChecked, currentSelect);
    }

    private Delete parseDelete() throws SQLException {

        final Delete command = new Delete(session, internalQuery);
        currentPrepared = command;
        final int start = lastParseIndex;
        readIf("FROM");
        final TableFilter filter = readSimpleTableFilter();
        command.setTableFilter(filter);
        if (readIf("WHERE")) {
            final Expression condition = readExpression();
            command.setCondition(condition);
        }
        setSQL(command, "DELETE", start);
        return command;
    }

    private IndexColumn[] parseIndexColumnList() throws SQLException {

        final ObjectArray columns = new ObjectArray();
        do {
            final IndexColumn column = new IndexColumn();
            column.columnName = readColumnIdentifier();
            columns.add(column);
            if (readIf("ASC")) {
                // ignore
            }
            else if (readIf("DESC")) {
                column.sortType = SortOrder.DESCENDING;
            }
            if (readIf("NULLS")) {
                if (readIf("FIRST")) {
                    column.sortType |= SortOrder.NULLS_FIRST;
                }
                else {
                    read("LAST");
                    column.sortType |= SortOrder.NULLS_LAST;
                }
            }
        }
        while (readIf(","));
        read(")");
        final IndexColumn[] cols = new IndexColumn[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private String[] parseColumnList() throws SQLException {

        final ObjectArray columns = new ObjectArray();
        do {
            final String columnName = readColumnIdentifier();
            columns.add(columnName);
        }
        while (readIfMore());
        final String[] cols = new String[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private Column[] parseColumnList(final Table table) throws SQLException {

        final ObjectArray columns = new ObjectArray();
        final java.util.Set<Column> set = new HashSet<Column>();
        if (!readIf(")")) {
            do {
                final Column column = table.getColumn(readColumnIdentifier());
                if (!set.add(column)) { throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getSQL()); }
                columns.add(column);
            }
            while (readIfMore());
        }
        final Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        return cols;
    }

    private boolean readIfMore() throws SQLException {

        if (readIf(",")) { return !readIf(")"); }
        read(")");
        return false;
    }

    private Prepared parseHelp() throws SQLException {

        final StringBuilder buff = new StringBuilder("SELECT * FROM INFORMATION_SCHEMA.HELP");
        int i = 0;
        final ObjectArray paramValues = new ObjectArray();
        while (currentTokenType != END) {
            final String s = currentToken;
            read();
            if (i == 0) {
                buff.append(" WHERE ");
            }
            else {
                buff.append(" AND ");
            }
            i++;
            buff.append("UPPER(TOPIC) LIKE ?");
            paramValues.add(ValueString.get("%" + s + "%"));
        }
        return prepare(session, buff.toString(), paramValues);
    }

    private Prepared parseShow() throws SQLException {

        final ObjectArray paramValues = new ObjectArray();
        final StringBuilder buff = new StringBuilder("SELECT ");
        if (readIf("CLIENT_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UNICODE' AS CLIENT_ENCODING FROM DUAL");
        }
        else if (readIf("DATESTYLE")) {
            // for PostgreSQL compatibility
            buff.append("'ISO' AS DATESTYLE FROM DUAL");
        }
        else if (readIf("SERVER_VERSION")) {
            // for PostgreSQL compatibility
            buff.append("'8.1.4' AS SERVER_VERSION FROM DUAL");
        }
        else if (readIf("SERVER_ENCODING")) {
            // for PostgreSQL compatibility
            buff.append("'UTF8' AS SERVER_ENCODING FROM DUAL");
        }
        else if (readIf("TABLES")) {
            // for MySQL compatibility
            String schema = Constants.SCHEMA_MAIN;
            if (readIf("FROM")) {
                schema = readUniqueIdentifier();
            }
            buff.append("TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME");
            paramValues.add(ValueString.get(schema));
        }
        else if (readIf("COLUMNS")) {
            // for MySQL compatibility
            read("FROM");
            final String tableName = readUniqueIdentifier();
            paramValues.add(ValueString.get(tableName));
            String schema = Constants.SCHEMA_MAIN;
            if (readIf("FROM")) {
                schema = readUniqueIdentifier();
            }
            buff.append("COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=? ORDER BY ORDINAL_POSITION");
            paramValues.add(ValueString.get(schema));
        }
        else if (readIf("DATABASES") || readIf("SCHEMAS")) {
            // for MySQL compatibility
            buff.append("SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        }
        return prepare(session, buff.toString(), paramValues);
    }

    private Prepared prepare(final Session session, final String sql, final ObjectArray paramValues) throws SQLException {

        final Prepared prep = session.prepare(sql);
        final ObjectArray params = prep.getParameters();
        for (int i = 0; params != null && i < params.size(); i++) {
            final Parameter p = (Parameter) params.get(i);
            p.setValue((Value) paramValues.get(i));
        }
        return prep;
    }

    private Merge parseMerge() throws SQLException {

        final Merge command = new Merge(session, internalQuery);
        currentPrepared = command;
        read("INTO");
        final Table table = readTableOrView();
        command.setTable(table);
        if (readIf("(")) {
            final Column[] columns = parseColumnList(table);
            command.setColumns(columns);
        }
        if (readIf("KEY")) {
            read("(");
            final Column[] keys = parseColumnList(table);
            command.setKeys(keys);
        }
        if (readIf("VALUES")) {
            do {
                final ObjectArray values = new ObjectArray();
                read("(");
                if (!readIf(")")) {
                    do {
                        if (readIf("DEFAULT")) {
                            values.add(null);
                        }
                        else {
                            values.add(readExpression());
                        }
                    }
                    while (readIfMore());
                }
                final Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            }
            while (readIf(","));
        }
        else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private Prepared parseInsert() throws SQLException {

        final Insert command = new Insert(session, internalQuery);
        currentPrepared = command;
        read("INTO");
        final Table table = readTableOrView();

        command.setTable(table);
        if (readIf("(")) {
            if (isToken("SELECT") || isToken("FROM")) {
                command.setQuery(parseSelect());
                read(")");
                return command;
            }
            else {
                final Column[] columns = parseColumnList(table);
                command.setColumns(columns);
            }
        }
        if (readIf("DEFAULT")) {
            read("VALUES");
            final Expression[] expr = new Expression[0];
            command.addRow(expr);
        }
        else if (readIf("VALUES")) {
            do {
                final ObjectArray values = new ObjectArray();
                read("(");
                if (!readIf(")")) {
                    do {
                        if (readIf("DEFAULT")) {
                            values.add(null);
                        }
                        else {
                            values.add(readExpression());
                        }
                    }
                    while (readIfMore());
                }
                final Expression[] expr = new Expression[values.size()];
                values.toArray(expr);
                command.addRow(expr);
            }
            while (readIf(","));
        }
        else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private TableFilter readTableFilter(final boolean fromOuter) throws SQLException {

        Table table;
        String alias = null;
        if (readIf("(")) {
            if (isToken("SELECT") || isToken("FROM")) {
                final int start = lastParseIndex;
                final int paramIndex = parameters.size();
                Query query = parseSelectUnion();
                read(")");
                query = parseSelectUnionExtension(query, start, true);
                final ObjectArray params = new ObjectArray();
                for (int i = paramIndex; i < parameters.size(); i++) {
                    params.add(parameters.get(i));
                }
                query.setParameterList(params);
                query.init();
                Session s;
                if (prepared != null && prepared instanceof CreateView) {
                    s = database.getSystemSession();
                }
                else {
                    s = session;
                }
                alias = session.getNextSystemIdentifier(sqlCommand);
                table = TableView.createTempView(s, session.getUser(), alias, query, currentSelect);
            }
            else {
                TableFilter top = readTableFilter(fromOuter);
                top = readJoin(top, currentSelect, fromOuter);
                read(")");
                alias = readFromAlias(null);
                if (alias != null) {
                    top.setAlias(alias);
                }
                return top;
            }
        }
        else {
            final String tableName = readIdentifierWithSchema(null);
            if (readIf("(")) {
                final Schema mainSchema = database.getSchema(Constants.SCHEMA_MAIN);
                if (tableName.equals(RangeTable.NAME)) {
                    final Expression min = readExpression();
                    read(",");
                    final Expression max = readExpression();
                    read(")");
                    table = new RangeTable(mainSchema, min, max);
                }
                else {
                    final Expression func = readFunction(tableName);
                    if (!(func instanceof FunctionCall)) { throw getSyntaxError(); }
                    table = new FunctionTable(mainSchema, session, func, (FunctionCall) func);
                }
            }
            else if ("DUAL".equals(tableName)) {
                table = getDualTable();
            }
            else {
                table = readTableOrView(tableName, true, currentSelect.getLocationPreference(), true);
            }
        }
        alias = readFromAlias(alias);
        return new TableFilter(session, table, alias, rightsChecked, currentSelect);
    }

    private String readFromAlias(String alias) throws SQLException {

        if (readIf("AS")) {
            alias = readAliasIdentifier();
        }
        else if (currentTokenType == IDENTIFIER) {
            // left and right are not keywords (because they are functions as
            // well)
            if (!isToken("LEFT") && !isToken("RIGHT") && !isToken("FULL")) {
                alias = readAliasIdentifier();
            }
        }
        return alias;
    }

    private Prepared parseTruncate() throws SQLException {

        read("TABLE");
        final Table table = readTableOrView();
        final TruncateTable command = new TruncateTable(session, internalQuery);
        command.setTable(table);
        return command;
    }

    private boolean readIfExists(boolean ifExists) throws SQLException {

        if (readIf("IF")) {
            read("EXISTS");
            ifExists = true;
        }
        return ifExists;
    }

    private Prepared parseComment() throws SQLException {

        int type = 0;
        read("ON");
        boolean column = false;
        if (readIf("TABLE") || readIf("VIEW")) {
            type = DbObject.TABLE_OR_VIEW;
        }
        else if (readIf("COLUMN")) {
            column = true;
            type = DbObject.TABLE_OR_VIEW;
        }
        else if (readIf("CONSTANT")) {
            type = DbObject.CONSTANT;
        }
        else if (readIf("CONSTRAINT")) {
            type = DbObject.CONSTRAINT;
        }
        else if (readIf("ALIAS")) {
            type = DbObject.FUNCTION_ALIAS;
        }
        else if (readIf("INDEX")) {
            type = DbObject.INDEX;
        }
        else if (readIf("ROLE")) {
            type = DbObject.ROLE;
        }
        else if (readIf("SCHEMA")) {
            type = DbObject.SCHEMA;
        }
        else if (readIf("SEQUENCE")) {
            type = DbObject.SEQUENCE;
        }
        else if (readIf("TRIGGER")) {
            type = DbObject.TRIGGER;
        }
        else if (readIf("USER")) {
            type = DbObject.USER;
        }
        else if (readIf("DOMAIN")) {
            type = DbObject.USER_DATATYPE;
        }
        else {
            throw getSyntaxError();
        }
        final SetComment command = new SetComment(session);
        String objectName = readIdentifierWithSchema();
        if (column) {
            String columnName = objectName;
            objectName = schemaName;
            schemaName = session.getCurrentSchemaName();
            if (readIf(".")) {
                schemaName = objectName;
                objectName = columnName;
                columnName = readUniqueIdentifier();
            }
            command.setColumn(true);
            command.setColumnName(columnName);
        }
        command.setSchemaName(schemaName);
        command.setObjectName(objectName);
        command.setObjectType(type);
        read("IS");
        command.setCommentExpression(readExpression());
        return command;
    }

    private Prepared parseDrop() throws SQLException {

        if (readIf("TABLE")) {
            boolean ifExists = readIfExists(false);
            String tableName = readIdentifierWithSchema();
            final DropTable command = new DropTable(session, getSchema(), internalQuery);
            command.setTableName(tableName);
            while (readIf(",")) {
                tableName = readIdentifierWithSchema();
                final DropTable next = new DropTable(session, getSchema(), internalQuery);
                next.setTableName(tableName);
                command.addNextDropTable(next);
            }
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            if (readIf("CASCADE")) {
                readIf("CONSTRAINTS");
            }
            return command;
        }
        else if (readIf("REPLICA")) {
            boolean ifExists = readIfExists(false);
            String tableName = readIdentifierWithSchema();
            final DropReplica command = new DropReplica(session, getSchema());
            command.setTableName(tableName);
            while (readIf(",")) {
                tableName = readIdentifierWithSchema();
                final DropReplica next = new DropReplica(session, getSchema());
                next.setTableName(tableName);
                command.addNextDropTable(next);
            }
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);

            return command;
        }
        else if (readIf("INDEX")) {
            boolean ifExists = readIfExists(false);
            final String indexName = readIdentifierWithSchema();
            final DropIndex command = new DropIndex(session, getSchema());
            command.setIndexName(indexName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("USER")) {
            boolean ifExists = readIfExists(false);
            final DropUser command = new DropUser(session);
            command.setUserName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            readIf("CASCADE");
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("SEQUENCE")) {
            boolean ifExists = readIfExists(false);
            final String sequenceName = readIdentifierWithSchema();
            final DropSequence command = new DropSequence(session, getSchema());
            command.setSequenceName(sequenceName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("CONSTANT")) {
            boolean ifExists = readIfExists(false);
            final String constantName = readIdentifierWithSchema();
            final DropConstant command = new DropConstant(session, getSchema());
            command.setConstantName(constantName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("TRIGGER")) {
            boolean ifExists = readIfExists(false);
            final String triggerName = readIdentifierWithSchema();
            final DropTrigger command = new DropTrigger(session, getSchema());
            command.setTriggerName(triggerName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("VIEW")) {
            boolean ifExists = readIfExists(false);
            final String viewName = readIdentifierWithSchema();
            final DropView command = new DropView(session, getSchema());
            command.setViewName(viewName);
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("ROLE")) {
            boolean ifExists = readIfExists(false);
            final DropRole command = new DropRole(session);
            command.setRoleName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
            // TODO role: support role names SELECT | DELETE | INSERT | UPDATE |
            // ALL? does quoting work?
        }
        else if (readIf("ALIAS")) {
            boolean ifExists = readIfExists(false);
            final DropFunctionAlias command = new DropFunctionAlias(session);
            command.setAliasName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("SCHEMA")) {
            boolean ifExists = readIfExists(false);
            final DropSchema command = new DropSchema(session);
            command.setSchemaName(readUniqueIdentifier());
            ifExists = readIfExists(ifExists);
            command.setIfExists(ifExists);
            return command;
        }
        else if (readIf("ALL")) {
            read("OBJECTS");
            final DropDatabase command = new DropDatabase(session);
            command.setDropAllObjects(true);
            if (readIf("DELETE")) {
                read("FILES");
                command.setDeleteFiles(true);
            }
            return command;
        }
        else if (readIf("DOMAIN")) {
            return parseDropUserDataType();
        }
        else if (readIf("TYPE")) {
            return parseDropUserDataType();
        }
        else if (readIf("DATATYPE")) {
            return parseDropUserDataType();
        }
        else if (readIf("AGGREGATE")) { return parseDropAggregate(); }
        throw getSyntaxError();
    }

    private DropUserDataType parseDropUserDataType() throws SQLException {

        boolean ifExists = readIfExists(false);
        final DropUserDataType command = new DropUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private DropAggregate parseDropAggregate() throws SQLException {

        boolean ifExists = readIfExists(false);
        final DropAggregate command = new DropAggregate(session);
        command.setName(readUniqueIdentifier());
        ifExists = readIfExists(ifExists);
        command.setIfExists(ifExists);
        return command;
    }

    private TableFilter readJoin(TableFilter top, final Select command, final boolean fromOuter) throws SQLException {

        TableFilter last = top;
        while (true) {
            if (readIf("RIGHT")) {
                readIf("OUTER");
                read("JOIN");
                // the right hand side is the 'inner' table usually
                TableFilter newTop = readTableFilter(fromOuter);
                newTop = readJoin(newTop, command, true);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                newTop.addJoin(top, true, on);
                top = newTop;
                last = newTop;
            }
            else if (readIf("LEFT")) {
                readIf("OUTER");
                read("JOIN");
                final TableFilter join = readTableFilter(true);
                top = readJoin(top, command, true);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, true, on);
                last = join;
            }
            else if (readIf("FULL")) {
                throw getSyntaxError();
            }
            else if (readIf("INNER")) {
                read("JOIN");
                final TableFilter join = readTableFilter(fromOuter);
                top = readJoin(top, command, false);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            }
            else if (readIf("JOIN")) {
                final TableFilter join = readTableFilter(fromOuter);
                top = readJoin(top, command, false);
                Expression on = null;
                if (readIf("ON")) {
                    on = readExpression();
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            }
            else if (readIf("CROSS")) {
                read("JOIN");
                final TableFilter join = readTableFilter(fromOuter);
                top.addJoin(join, fromOuter, null);
                last = join;
            }
            else if (readIf("NATURAL")) {
                read("JOIN");
                final TableFilter join = readTableFilter(fromOuter);
                final Column[] tableCols = last.getTable().getColumns();
                final Column[] joinCols = join.getTable().getColumns();
                final String tableSchema = last.getTable().getSchema().getName();
                final String joinSchema = join.getTable().getSchema().getName();
                Expression on = null;
                for (final Column tableCol : tableCols) {
                    final String tableColumnName = tableCol.getName();
                    for (final Column c : joinCols) {
                        final String joinColumnName = c.getName();
                        if (tableColumnName.equals(joinColumnName)) {
                            join.addNaturalJoinColumn(c);
                            final Expression tableExpr = new ExpressionColumn(database, tableSchema, last.getTableAlias(), tableColumnName);
                            final Expression joinExpr = new ExpressionColumn(database, joinSchema, join.getTableAlias(), joinColumnName);
                            final Expression equal = new Comparison(session, Comparison.EQUAL, tableExpr, joinExpr);
                            if (on == null) {
                                on = equal;
                            }
                            else {
                                on = new ConditionAndOr(ConditionAndOr.AND, on, equal);
                            }
                        }
                    }
                }
                top.addJoin(join, fromOuter, on);
                last = join;
            }
            else {
                break;
            }
        }
        return top;
    }

    private Prepared parseExecute() throws SQLException {

        final ExecuteProcedure command = new ExecuteProcedure(session, internalQuery);
        final String procedureName = readAliasIdentifier();
        final Procedure p = session.getProcedure(procedureName);
        if (p == null) { throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1, procedureName); }
        command.setProcedure(p);
        if (readIf("(")) {
            for (int i = 0;; i++) {
                command.setExpression(i, readExpression());
                if (readIf(")")) {
                    break;
                }
                read(",");
            }
        }
        return command;
    }

    private DeallocateProcedure parseDeallocate() throws SQLException {

        readIf("PLAN");
        final String procedureName = readAliasIdentifier();
        final DeallocateProcedure command = new DeallocateProcedure(session);
        command.setProcedureName(procedureName);
        return command;
    }

    private ExplainPlan parseExplain() throws SQLException {

        final ExplainPlan command = new ExplainPlan(session, internalQuery);
        readIf("PLAN");
        readIf("FOR");
        if (isToken("SELECT") || isToken("FROM") || isToken("(")) {
            command.setCommand(parseSelect());
        }
        else if (readIf("DELETE")) {
            command.setCommand(parseDelete());
        }
        else if (readIf("UPDATE")) {
            command.setCommand(parseUpdate());
        }
        else if (readIf("INSERT")) {
            command.setCommand(parseInsert());
        }
        else if (readIf("MERGE")) {
            command.setCommand(parseMerge());
        }
        else {
            throw getSyntaxError();
        }
        return command;
    }

    private Query parseSelect() throws SQLException {

        final int paramIndex = parameters.size();
        final Query command = parseSelectUnion();
        final ObjectArray params = new ObjectArray();
        for (int i = paramIndex; i < parameters.size(); i++) {
            params.add(parameters.get(i));
        }
        command.setParameterList(params);
        command.init();
        return command;
    }

    private Query parseSelectUnion() throws SQLException {

        final int start = lastParseIndex;
        final Query command = parseSelectSub();
        return parseSelectUnionExtension(command, start, false);
    }

    private Query parseSelectUnionExtension(Query command, final int start, final boolean unionOnly) throws SQLException {

        while (true) {
            if (readIf("UNION")) {
                final SelectUnion union = new SelectUnion(session, command, internalQuery);
                if (readIf("ALL")) {
                    union.setUnionType(SelectUnion.UNION_ALL);
                }
                else {
                    readIf("DISTINCT");
                    union.setUnionType(SelectUnion.UNION);
                }
                union.setRight(parseSelectSub());
                command = union;
            }
            else if (readIf("MINUS") || readIf("EXCEPT")) {
                final SelectUnion union = new SelectUnion(session, command, internalQuery);
                union.setUnionType(SelectUnion.EXCEPT);
                union.setRight(parseSelectSub());
                command = union;
            }
            else if (readIf("INTERSECT")) {
                final SelectUnion union = new SelectUnion(session, command, internalQuery);
                union.setUnionType(SelectUnion.INTERSECT);
                union.setRight(parseSelectSub());
                command = union;
            }
            else {
                break;
            }
        }
        if (!unionOnly) {
            parseEndOfQuery(command);
        }
        setSQL(command, null, start);
        return command;
    }

    private void parseEndOfQuery(final Query command) throws SQLException {

        if (readIf("ORDER")) {
            read("BY");
            final Select oldSelect = currentSelect;
            if (command instanceof Select) {
                currentSelect = (Select) command;
            }
            final ObjectArray orderList = new ObjectArray();
            do {
                boolean canBeNumber = true;
                if (readIf("=")) {
                    canBeNumber = false;
                }
                final SelectOrderBy order = new SelectOrderBy();
                final Expression expr = readExpression();
                if (canBeNumber && expr instanceof ValueExpression && expr.getType() == Value.INT) {
                    order.columnIndexExpr = expr;
                }
                else if (expr instanceof Parameter) {
                    recompileAlways = true;
                    order.columnIndexExpr = expr;
                }
                else {
                    order.expression = expr;
                }
                if (readIf("DESC")) {
                    order.descending = true;
                }
                else {
                    readIf("ASC");
                }
                if (readIf("NULLS")) {
                    if (readIf("FIRST")) {
                        order.nullsFirst = true;
                    }
                    else {
                        read("LAST");
                        order.nullsLast = true;
                    }
                }
                orderList.add(order);
            }
            while (readIf(","));
            command.setOrder(orderList);
            currentSelect = oldSelect;
        }
        if (database.getMode().supportOffsetFetch) {
            // make sure aggregate functions will not work here
            final Select temp = currentSelect;
            currentSelect = null;

            // http://sqlpro.developpez.com/SQL2008/
            if (readIf("OFFSET")) {
                command.setOffset(readExpression().optimize(session));
                if (!readIf("ROW")) {
                    read("ROWS");
                }
            }
            if (readIf("FETCH")) {
                read("FIRST");
                if (readIf("ROW")) {
                    command.setLimit(ValueExpression.get(ValueInt.get(1)));
                }
                else {
                    final Expression limit = readExpression().optimize(session);
                    command.setLimit(limit);
                    if (!readIf("ROW")) {
                        read("ROWS");
                    }
                }
                read("ONLY");
            }

            currentSelect = temp;
        }
        if (readIf("LIMIT")) {
            final Select temp = currentSelect;
            // make sure aggregate functions will not work here
            currentSelect = null;
            Expression limit = readExpression().optimize(session);
            command.setLimit(limit);
            if (readIf("OFFSET")) {
                final Expression offset = readExpression().optimize(session);
                command.setOffset(offset);
            }
            else if (readIf(",")) {
                // MySQL: [offset, ] rowcount
                final Expression offset = limit;
                limit = readExpression().optimize(session);
                command.setOffset(offset);
                command.setLimit(limit);
            }
            if (readIf("SAMPLE_SIZE")) {
                command.setSampleSize(getPositiveInt());
            }
            currentSelect = temp;
        }
        if (readIf("FOR")) {
            if (readIf("UPDATE")) {
                if (readIf("OF")) {
                    do {
                        readIdentifierWithSchema();
                    }
                    while (readIf(","));
                }
                else if (readIf("NOWAIT")) {
                    // TODO parser: select for update nowait: should not wait
                }
                else if (readIf("WITH")) {
                    // Hibernate / Derby support
                    read("RR");
                }
                command.setForUpdate(true);
            }
            else if (readIf("READ")) {
                read("ONLY");
                if (readIf("WITH")) {
                    read("RS");
                }
            }
        }
    }

    private Query parseSelectSub() throws SQLException {

        if (readIf("(")) {
            final Query command = parseSelectUnion();
            read(")");
            return command;
        }
        final Select select = parseSelectSimple();
        return select;
    }

    private void parseSelectSimpleFromPart(final Select command) throws SQLException {

        do {
            final TableFilter filter = readTableFilter(false);
            parseJoinTableFilter(filter, command);
        }
        while (readIf(","));
    }

    private void parseJoinTableFilter(TableFilter top, final Select command) throws SQLException {

        top = readJoin(top, command, top.isJoinOuter());
        command.addTableFilter(top, true);
        boolean isOuter = false;
        while (true) {
            final TableFilter join = top.getJoin();
            if (join == null) {
                break;
            }
            isOuter = isOuter | join.isJoinOuter();
            if (isOuter) {
                command.addTableFilter(join, false);
            }
            else {
                // make flat so the optimizer can work better
                final Expression on = join.getJoinCondition();
                if (on != null) {
                    command.addCondition(on);
                }
                join.removeJoinCondition();
                top.removeJoin();
                command.addTableFilter(join, true);
            }
            top = join;
        }
    }

    private void parseSelectSimpleSelectPart(final Select command) throws SQLException {

        final Select temp = currentSelect;
        // make sure aggregate functions will not work in TOP and LIMIT
        currentSelect = null;
        if (readIf("TOP")) {
            // can't read more complex expressions here because
            // SELECT TOP 1 +? A FROM TEST could mean
            // SELECT TOP (1+?) A FROM TEST or
            // SELECT TOP 1 (+?) AS A FROM TEST
            final Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        }
        else if (readIf("LIMIT")) {
            final Expression offset = readTerm().optimize(session);
            command.setOffset(offset);
            final Expression limit = readTerm().optimize(session);
            command.setLimit(limit);
        }
        currentSelect = temp;
        if (readIf("DISTINCT")) {
            command.setDistinct(true);
        }
        else if (readIf("LOCAL")) {
            if (readIf("ONLY")) {
                command.setLocationPreference(LocationPreference.LOCAL_STRICT);
            }
            else {
                command.setLocationPreference(LocationPreference.LOCAL);
            }
        }
        else if (readIf("PRIMARY")) {
            if (readIf("ONLY")) {
                command.setLocationPreference(LocationPreference.PRIMARY_STRICT);
            }
            else {
                command.setLocationPreference(LocationPreference.PRIMARY);
            }
        }
        else {
            readIf("ALL");
        }
        final ObjectArray expressions = new ObjectArray();
        do {
            if (readIf("*")) {
                expressions.add(new Wildcard(null, null));
            }
            else {
                Expression expr = readExpression();
                if (readIf("AS") || currentTokenType == IDENTIFIER) {
                    final String alias = readAliasIdentifier();
                    expr = new Alias(expr, alias, database.getMode().aliasColumnName);
                }
                expressions.add(expr);
            }
        }
        while (readIf(","));
        command.setExpressions(expressions);
    }

    private Select parseSelectSimple() throws SQLException {

        boolean fromFirst;
        if (readIf("SELECT")) {
            fromFirst = false;
        }
        else if (readIf("FROM")) {
            fromFirst = true;
        }
        else {
            throw getSyntaxError();
        }
        final Select command = new Select(session, internalQuery);
        final int start = lastParseIndex;
        final Select oldSelect = currentSelect;
        currentSelect = command;
        currentPrepared = command;
        if (fromFirst) {
            parseSelectSimpleFromPart(command);
            read("SELECT");
            parseSelectSimpleSelectPart(command);
        }
        else {
            parseSelectSimpleSelectPart(command);
            if (!readIf("FROM")) {
                // select without FROM: convert to SELECT ... FROM
                // SYSTEM_RANGE(1,1)
                final Table dual = getDualTable();
                final TableFilter filter = new TableFilter(session, dual, null, rightsChecked, currentSelect);
                command.addTableFilter(filter, true);
            }
            else {
                parseSelectSimpleFromPart(command);
            }
        }
        if (readIf("WHERE")) {
            final Expression condition = readExpression();
            command.addCondition(condition);
        }
        // the group by is read for the outer select (or not a select)
        // so that columns that are not grouped can be used
        currentSelect = oldSelect;
        if (readIf("GROUP")) {
            read("BY");
            command.setGroupQuery();
            final ObjectArray list = new ObjectArray();
            do {
                final Expression expr = readExpression();
                list.add(expr);
            }
            while (readIf(","));
            command.setGroupBy(list);
        }
        currentSelect = command;
        if (readIf("HAVING")) {
            command.setGroupQuery();
            final Expression condition = readExpression();
            command.setHaving(condition);
        }
        command.setParameterList(parameters);
        currentSelect = oldSelect;
        setSQL(command, "SELECT", start);
        return command;
    }

    private Table getDualTable() throws SQLException {

        final Schema main = database.findSchema(Constants.SCHEMA_MAIN);
        final Expression one = ValueExpression.get(ValueLong.get(1));
        return new RangeTable(main, one, one);
    }

    private void setSQL(final Prepared command, final String start, final int startIndex) {

        String sql = originalSQL.substring(startIndex, lastParseIndex).trim();
        if (start != null) {
            sql = start + " " + sql;
        }
        command.setSQL(sql);
    }

    private Expression readExpression() throws SQLException {

        Expression r = readAnd();
        while (readIf("OR")) {
            r = new ConditionAndOr(ConditionAndOr.OR, r, readAnd());
        }
        return r;
    }

    private Expression readAnd() throws SQLException {

        Expression r = readCondition();
        while (readIf("AND")) {
            r = new ConditionAndOr(ConditionAndOr.AND, r, readCondition());
        }
        return r;
    }

    private Expression readCondition() throws SQLException {

        // TODO parser: should probably use switch case for performance
        if (readIf("NOT")) { return new ConditionNot(readCondition()); }
        if (readIf("EXISTS")) {
            read("(");
            final Query query = parseSelect();
            // can not reduce expression because it might be a union except
            // query with distinct
            read(")");
            return new ConditionExists(query);
        }
        Expression r = readConcat();
        while (true) {
            // special case: NOT NULL is not part of an expression (as in CREATE
            // TABLE TEST(ID INT DEFAULT 0 NOT NULL))
            final int backup = parseIndex;
            boolean not = false;
            if (readIf("NOT")) {
                not = true;
                if (isToken("NULL")) {
                    // this really only works for NOT NULL!
                    parseIndex = backup;
                    currentToken = "NOT";
                    break;
                }
            }
            if (readIf("LIKE")) {
                final Expression b = readConcat();
                Expression esc = null;
                if (readIf("ESCAPE")) {
                    esc = readConcat();
                }
                recompileAlways = true;
                r = new CompareLike(database.getCompareMode(), r, b, esc, false);
            }
            else if (readIf("REGEXP")) {
                final Expression b = readConcat();
                r = new CompareLike(database.getCompareMode(), r, b, null, true);
            }
            else if (readIf("IS")) {
                int type;
                if (readIf("NOT")) {
                    type = Comparison.IS_NOT_NULL;
                }
                else {
                    type = Comparison.IS_NULL;
                }
                read("NULL");
                r = new Comparison(session, type, r, null);
            }
            else if (readIf("IN")) {
                if (SysProperties.OPTIMIZE_IN) {
                    recompileAlways = true;
                }
                read("(");
                if (readIf(")")) {
                    r = ValueExpression.get(ValueBoolean.get(false));
                }
                else {
                    if (isToken("SELECT") || isToken("FROM")) {
                        final Query query = parseSelect();
                        r = new ConditionInSelect(database, r, query, false, Comparison.EQUAL);
                    }
                    else {
                        final ObjectArray v = new ObjectArray();
                        Expression last;
                        do {
                            last = readExpression();
                            v.add(last);
                        }
                        while (readIf(","));
                        if (v.size() == 1 && last instanceof Subquery) {
                            final Subquery s = (Subquery) last;
                            final Query q = s.getQuery();
                            r = new ConditionInSelect(database, r, q, false, Comparison.EQUAL);
                        }
                        else {
                            r = new ConditionIn(database, r, v);
                        }
                    }
                    read(")");
                }
            }
            else if (readIf("BETWEEN")) {
                final Expression low = readConcat();
                read("AND");
                final Expression high = readConcat();
                final Expression condLow = new Comparison(session, Comparison.SMALLER_EQUAL, low, r);
                final Expression condHigh = new Comparison(session, Comparison.BIGGER_EQUAL, high, r);
                r = new ConditionAndOr(ConditionAndOr.AND, condLow, condHigh);
            }
            else {
                // TODO parser: if we use a switch case, we don't need
                // getCompareType any more
                final int compareType = getCompareType(currentTokenType);
                if (compareType < 0) {
                    break;
                }
                read();
                if (readIf("ALL")) {
                    read("(");
                    final Query query = parseSelect();
                    r = new ConditionInSelect(database, r, query, true, compareType);
                    read(")");
                }
                else if (readIf("ANY") || readIf("SOME")) {
                    read("(");
                    final Query query = parseSelect();
                    r = new ConditionInSelect(database, r, query, false, compareType);
                    read(")");
                }
                else {
                    final Expression right = readConcat();
                    if (readIf("(") && readIf("+") && readIf(")")) {
                        // support for a subset of old-fashioned Oracle outer
                        // join with (+)
                        if (r instanceof ExpressionColumn && right instanceof ExpressionColumn) {
                            final ExpressionColumn leftCol = (ExpressionColumn) r;
                            final ExpressionColumn rightCol = (ExpressionColumn) right;
                            final ObjectArray filters = currentSelect.getTopFilters();
                            for (int i = 0; filters != null && i < filters.size(); i++) {
                                TableFilter f = (TableFilter) filters.get(i);
                                while (f != null) {
                                    leftCol.mapColumns(f, 0);
                                    rightCol.mapColumns(f, 0);
                                    f = f.getJoin();
                                }
                            }
                            final TableFilter leftFilter = leftCol.getTableFilter();
                            final TableFilter rightFilter = rightCol.getTableFilter();
                            r = new Comparison(session, compareType, r, right);
                            if (leftFilter != null && rightFilter != null) {
                                final int idx = filters.indexOf(rightFilter);
                                if (idx >= 0) {
                                    filters.remove(idx);
                                    leftFilter.addJoin(rightFilter, true, r);
                                }
                                else {
                                    rightFilter.mapAndAddFilter(r);
                                }
                                r = ValueExpression.get(ValueBoolean.get(true));
                            }
                        }
                    }
                    else {
                        r = new Comparison(session, compareType, r, right);
                    }
                }
            }
            if (not) {
                r = new ConditionNot(r);
            }
        }
        return r;
    }

    private Expression readConcat() throws SQLException {

        Expression r = readSum();
        while (true) {
            if (readIf("||")) {
                r = new Operation(Operation.CONCAT, r, readSum());
            }
            else if (readIf("~")) {
                if (readIf("*")) {
                    final Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X", Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new CompareLike(database.getCompareMode(), r, readSum(), null, true);
            }
            else if (readIf("!~")) {
                if (readIf("*")) {
                    final Function function = Function.getFunction(database, "CAST");
                    function.setDataType(new Column("X", Value.STRING_IGNORECASE));
                    function.setParameter(0, r);
                    r = function;
                }
                r = new ConditionNot(new CompareLike(database.getCompareMode(), r, readSum(), null, true));
            }
            else {
                return r;
            }
        }
    }

    private Expression readSum() throws SQLException {

        Expression r = readFactor();
        while (true) {
            if (readIf("+")) {
                r = new Operation(Operation.PLUS, r, readFactor());
            }
            else if (readIf("-")) {
                r = new Operation(Operation.MINUS, r, readFactor());
            }
            else {
                return r;
            }
        }
    }

    private Expression readFactor() throws SQLException {

        Expression r = readTerm();
        while (true) {
            if (readIf("*")) {
                r = new Operation(Operation.MULTIPLY, r, readTerm());
            }
            else if (readIf("/")) {
                r = new Operation(Operation.DIVIDE, r, readTerm());
            }
            else {
                return r;
            }
        }
    }

    private Expression readAggregate(final int aggregateType) throws SQLException {

        if (currentSelect == null) { throw getSyntaxError(); }
        currentSelect.setGroupQuery();
        Expression r;
        if (aggregateType == Aggregate.COUNT) {
            if (readIf("*")) {
                r = new Aggregate(Aggregate.COUNT_ALL, null, currentSelect, false);
            }
            else {
                final boolean distinct = readIf("DISTINCT");
                final Expression on = readExpression();
                if (on instanceof Wildcard && !distinct) {
                    // PostgreSQL compatibility: count(t.*)
                    r = new Aggregate(Aggregate.COUNT_ALL, null, currentSelect, false);
                }
                else {
                    r = new Aggregate(Aggregate.COUNT, on, currentSelect, distinct);
                }
            }
        }
        else if (aggregateType == Aggregate.GROUP_CONCAT) {
            final boolean distinct = readIf("DISTINCT");
            final Aggregate agg = new Aggregate(Aggregate.GROUP_CONCAT, readExpression(), currentSelect, distinct);
            if (readIf("ORDER")) {
                read("BY");
                agg.setOrder(parseSimpleOrderList());
            }
            if (readIf("SEPARATOR")) {
                agg.setSeparator(readExpression());
            }
            r = agg;
        }
        else {
            final boolean distinct = readIf("DISTINCT");
            r = new Aggregate(aggregateType, readExpression(), currentSelect, distinct);
        }
        read(")");
        return r;
    }

    private ObjectArray parseSimpleOrderList() throws SQLException {

        final ObjectArray orderList = new ObjectArray();
        do {
            final SelectOrderBy order = new SelectOrderBy();
            final Expression expr = readExpression();
            order.expression = expr;
            if (readIf("DESC")) {
                order.descending = true;
            }
            else {
                readIf("ASC");
            }
            orderList.add(order);
        }
        while (readIf(","));
        return orderList;
    }

    private JavaFunction readJavaFunction(final String name) throws SQLException {

        final FunctionAlias functionAlias = database.findFunctionAlias(name);
        if (functionAlias == null) {
            // TODO compatibility: support 'on the fly java functions' as HSQLDB
            // ( CALL "java.lang.Math.sqrt"(2.0) )
            throw Message.getSQLException(ErrorCode.FUNCTION_NOT_FOUND_1, name);
        }
        Expression[] args;
        final ObjectArray argList = new ObjectArray();
        int numArgs = 0;
        while (!readIf(")")) {
            if (numArgs++ > 0) {
                read(",");
            }
            argList.add(readExpression());
        }
        args = new Expression[numArgs];
        argList.toArray(args);
        final JavaFunction func = new JavaFunction(functionAlias, args);
        return func;
    }

    private JavaAggregate readJavaAggregate(final UserAggregate aggregate) throws SQLException {

        final ObjectArray params = new ObjectArray();
        do {
            params.add(readExpression());
        }
        while (readIf(","));
        read(")");
        final Expression[] list = new Expression[params.size()];
        params.toArray(list);
        final JavaAggregate agg = new JavaAggregate(aggregate, list, currentSelect);
        currentSelect.setGroupQuery();
        return agg;
    }

    private Expression readFunction(final String name) throws SQLException {

        final int agg = Aggregate.getAggregateType(name);
        if (agg >= 0) { return readAggregate(agg); }
        Function function = Function.getFunction(database, name);
        if (function == null) {
            final UserAggregate aggregate = database.findAggregate(name);
            if (aggregate != null) { return readJavaAggregate(aggregate); }
            return readJavaFunction(name);
        }
        switch (function.getFunctionType()) {
            case Function.CAST: {
                function.setParameter(0, readExpression());
                read("AS");
                final Column type = parseColumn(null);
                function.setDataType(type);
                read(")");
                break;
            }
            case Function.CONVERT: {
                function.setParameter(0, readExpression());
                read(",");
                final Column type = parseColumn(null);
                function.setDataType(type);
                read(")");
                break;
            }
            case Function.EXTRACT: {
                function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
                read();
                read("FROM");
                function.setParameter(1, readExpression());
                read(")");
                break;
            }
            case Function.DATE_DIFF: {
                if (Function.isDatePart(currentToken)) {
                    function.setParameter(0, ValueExpression.get(ValueString.get(currentToken)));
                    read();
                }
                else {
                    function.setParameter(0, readExpression());
                }
                read(",");
                function.setParameter(1, readExpression());
                read(",");
                function.setParameter(2, readExpression());
                read(")");
                break;
            }
            case Function.SUBSTRING: {
                function.setParameter(0, readExpression());
                if (!readIf(",")) {
                    read("FROM");
                }
                function.setParameter(1, readExpression());
                if (readIf("FOR") || readIf(",")) {
                    function.setParameter(2, readExpression());
                }
                read(")");
                break;
            }
            case Function.POSITION: {
                // can't read expression because IN would be read too early
                function.setParameter(0, readConcat());
                if (!readIf(",")) {
                    read("IN");
                }
                function.setParameter(1, readExpression());
                read(")");
                break;
            }
            case Function.TRIM: {
                Expression space = null;
                if (readIf("LEADING")) {
                    function = Function.getFunction(database, "LTRIM");
                    if (!readIf("FROM")) {
                        space = readExpression();
                        read("FROM");
                    }
                }
                else if (readIf("TRAILING")) {
                    function = Function.getFunction(database, "RTRIM");
                    if (!readIf("FROM")) {
                        space = readExpression();
                        read("FROM");
                    }
                }
                else if (readIf("BOTH")) {
                    if (!readIf("FROM")) {
                        space = readExpression();
                        read("FROM");
                    }
                }
                Expression p0 = readExpression();
                if (readIf(",")) {
                    space = readExpression();
                }
                else if (readIf("FROM")) {
                    space = p0;
                    p0 = readExpression();
                }
                function.setParameter(0, p0);
                if (space != null) {
                    function.setParameter(1, space);
                }
                read(")");
                break;
            }
            case Function.TABLE:
            case Function.TABLE_DISTINCT: {
                int i = 0;
                final ObjectArray columns = new ObjectArray();
                do {
                    final String columnName = readAliasIdentifier();
                    final Column column = parseColumn(columnName);
                    columns.add(column);
                    read("=");
                    function.setParameter(i, readExpression());
                    i++;
                }
                while (readIf(","));
                read(")");
                final TableFunction tf = (TableFunction) function;
                tf.setColumns(columns);
                break;
            }
            default:
                if (!readIf(")")) {
                    int i = 0;
                    do {
                        function.setParameter(i++, readExpression());
                    }
                    while (readIf(","));
                    read(")");
                }
        }
        function.doneWithParameters();
        return function;
    }

    private Function readFunctionWithoutParameters(final String name) throws SQLException {

        if (readIf("(")) {
            read(")");
        }
        final Function function = Function.getFunction(database, name);
        function.doneWithParameters();
        return function;
    }

    private Expression readWildcardOrSequenceValue(String schema, final String objectName) throws SQLException {

        if (readIf("*")) { return new Wildcard(schema, objectName); }
        if (schema == null) {
            schema = session.getCurrentSchemaName();
        }
        if (readIf("NEXTVAL")) {
            final Sequence sequence = database.getSchema(schema).findSequence(objectName);
            if (sequence != null) { return new SequenceValue(sequence); }
        }
        else if (readIf("CURRVAL")) {
            final Sequence sequence = database.getSchema(schema).findSequence(objectName);
            if (sequence != null) {
                final Function function = Function.getFunction(database, "CURRVAL");
                function.setParameter(0, ValueExpression.get(ValueString.get(schema)));
                function.setParameter(1, ValueExpression.get(ValueString.get(objectName)));
                function.doneWithParameters();
                return function;
            }
        }
        return null;
    }

    private Expression readTermObjectDot(String objectName) throws SQLException {

        Expression expr = readWildcardOrSequenceValue(null, objectName);
        if (expr != null) { return expr; }
        String name = readColumnIdentifier();
        if (readIf(".")) {
            String schema = objectName;
            objectName = name;
            expr = readWildcardOrSequenceValue(schema, objectName);
            if (expr != null) { return expr; }
            name = readColumnIdentifier();
            if (readIf(".")) {
                final String databaseName = schema;
                if (!database.getShortName().equals(databaseName)) { throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, databaseName); }
                schema = objectName;
                objectName = name;
                expr = readWildcardOrSequenceValue(schema, objectName);
                if (expr != null) { return expr; }
                name = readColumnIdentifier();
                return new ExpressionColumn(database, schema, objectName, name);
            }
            return new ExpressionColumn(database, schema, objectName, name);
        }
        return new ExpressionColumn(database, null, objectName, name);
    }

    private Expression readTerm() throws SQLException {

        Expression r;
        switch (currentTokenType) {
            case AT:
                read();
                r = new Variable(session, readAliasIdentifier());
                if (readIf(":=")) {
                    final Expression value = readExpression();
                    final Function function = Function.getFunction(database, "SET");
                    function.setParameter(0, r);
                    function.setParameter(1, value);
                    r = function;
                }
                break;
            case PARAMETER:
                // there must be no space between ? and the number
                final boolean indexed = Character.isDigit(sqlCommandChars[parseIndex]);
                read();
                if (indexed && currentTokenType == VALUE && currentValue.getType() == Value.INT) {
                    if (indexedParameterList == null) {
                        if (parameters == null) {
                            // this can occur when parsing expressions only (for
                            // example check constraints)
                            throw getSyntaxError();
                        }
                        else if (parameters.size() > 0) { throw Message.getSQLException(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS); }
                        indexedParameterList = new ObjectArray();
                    }
                    final int index = currentValue.getInt() - 1;
                    if (index < 0 || index >= Constants.MAX_PARAMETER_INDEX) { throw Message.getInvalidValueException("" + index, "Parameter Index"); }
                    if (indexedParameterList.size() <= index) {
                        indexedParameterList.setSize(index + 1);
                    }
                    r = (Parameter) indexedParameterList.get(index);
                    if (r == null) {
                        r = new Parameter(index);
                        indexedParameterList.set(index, r);
                    }
                    read();
                }
                else {
                    if (indexedParameterList != null) { throw Message.getSQLException(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS); }
                    r = new Parameter(parameters.size());
                }
                parameters.add(r);
                break;
            case KEYWORD:
                if (isToken("SELECT") || isToken("FROM")) {
                    final Query query = parseSelect();
                    r = new Subquery(query);
                }
                else {
                    throw getSyntaxError();
                }
                break;
            case IDENTIFIER:
                final String name = currentToken;
                if (currentTokenQuoted) {
                    read();
                    if (readIf("(")) {
                        r = readFunction(name);
                    }
                    else if (readIf(".")) {
                        r = readTermObjectDot(name);
                    }
                    else {
                        r = new ExpressionColumn(database, null, null, name);
                    }
                }
                else {
                    read();
                    if ("X".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                        read();
                        final byte[] buffer = ByteUtils.convertStringToBytes(currentValue.getString());
                        r = ValueExpression.get(ValueBytes.getNoCopy(buffer));
                    }
                    else if (readIf(".")) {
                        r = readTermObjectDot(name);
                    }
                    else if ("CASE".equals(name)) {
                        // CASE must be processed before (,
                        // otherwise CASE(3) would be a function call, which it is
                        // not
                        if (isToken("WHEN")) {
                            r = readWhen(null);
                        }
                        else {
                            final Expression left = readExpression();
                            r = readWhen(left);
                        }
                    }
                    else if (readIf("(")) {
                        r = readFunction(name);
                    }
                    else if ("CURRENT_USER".equals(name)) {
                        r = readFunctionWithoutParameters("USER");
                    }
                    else if ("CURRENT".equals(name)) {
                        if (readIf("TIMESTAMP")) {
                            r = readFunctionWithoutParameters("CURRENT_TIMESTAMP");
                        }
                        else if (readIf("TIME")) {
                            r = readFunctionWithoutParameters("CURRENT_TIME");
                        }
                        else if (readIf("DATE")) {
                            r = readFunctionWithoutParameters("CURRENT_DATE");
                        }
                        else {
                            r = new ExpressionColumn(database, null, null, name);
                        }
                    }
                    else if ("NEXT".equals(name) && readIf("VALUE")) {
                        read("FOR");
                        final Sequence sequence = readSequence();
                        r = new SequenceValue(sequence);
                    }
                    else if ("DATE".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                        final String date = currentValue.getString();
                        read();
                        r = ValueExpression.get(ValueDate.get(ValueDate.parseDate(date)));
                    }
                    else if ("TIME".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                        final String time = currentValue.getString();
                        read();
                        r = ValueExpression.get(ValueTime.get(ValueTime.parseTime(time)));
                    }
                    else if ("TIMESTAMP".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                        final String timestamp = currentValue.getString();
                        read();
                        r = ValueExpression.get(ValueTimestamp.getNoCopy(ValueTimestamp.parseTimestamp(timestamp)));
                    }
                    else if ("E".equals(name) && currentTokenType == VALUE && currentValue.getType() == Value.STRING) {
                        final String text = currentValue.getString();
                        read();
                        r = ValueExpression.get(ValueString.get(text));
                    }
                    else {
                        r = new ExpressionColumn(database, null, null, name);
                    }
                }
                break;
            case MINUS:
                read();
                if (currentTokenType == VALUE) {
                    r = ValueExpression.get(currentValue.negate());
                    // convert Integer.MIN_VALUE to int (-Integer.MIN_VALUE needed
                    // to be a long)
                    if (r.getType() == Value.LONG && r.getValue(session).getLong() == Integer.MIN_VALUE) {
                        r = ValueExpression.get(ValueInt.get(Integer.MIN_VALUE));
                    }
                    read();
                }
                else {
                    r = new Operation(Operation.NEGATE, readTerm(), null);
                }
                break;
            case PLUS:
                read();
                r = readTerm();
                break;
            case OPEN:
                read();
                r = readExpression();
                if (readIf(",")) {
                    final ObjectArray list = new ObjectArray();
                    list.add(r);
                    do {
                        r = readExpression();
                        list.add(r);
                    }
                    while (readIf(","));
                    final Expression[] array = new Expression[list.size()];
                    list.toArray(array);
                    r = new ExpressionList(array);
                }
                read(")");
                break;
            case TRUE:
                read();
                r = ValueExpression.get(ValueBoolean.get(true));
                break;
            case FALSE:
                read();
                r = ValueExpression.get(ValueBoolean.get(false));
                break;
            case CURRENT_TIME:
                read();
                r = readFunctionWithoutParameters("CURRENT_TIME");
                break;
            case CURRENT_DATE:
                read();
                r = readFunctionWithoutParameters("CURRENT_DATE");
                break;
            case CURRENT_TIMESTAMP: {
                final Function function = Function.getFunction(database, "CURRENT_TIMESTAMP");
                read();
                if (readIf("(")) {
                    if (!readIf(")")) {
                        function.setParameter(0, readExpression());
                        read(")");
                    }
                }
                function.doneWithParameters();
                r = function;
                break;
            }
            case ROWNUM:
                read();
                if (readIf("(")) {
                    read(")");
                }
                r = new Rownum(currentSelect == null ? currentPrepared : currentSelect);
                break;
            case NULL:
                read();
                r = ValueExpression.getNull();
                break;
            case VALUE:
                r = ValueExpression.get(currentValue);
                read();
                break;
            default:
                throw getSyntaxError();
        }
        if (readIf("[")) {
            final Function function = Function.getFunction(database, "ARRAY_GET");
            function.setParameter(0, r);
            r = readExpression();
            r = new Operation(Operation.PLUS, r, ValueExpression.get(ValueInt.get(1)));
            function.setParameter(1, r);
            r = function;
            read("]");
        }
        if (readIf("::")) {
            // PostgreSQL compatibility
            final Column col = parseColumn(null);
            final Function function = Function.getFunction(database, "CAST");
            function.setDataType(col);
            function.setParameter(0, r);
            r = function;
        }
        return r;
    }

    private Expression readWhen(final Expression left) throws SQLException {

        if (readIf("END")) {
            readIf("CASE");
            return ValueExpression.getNull();
        }
        if (readIf("ELSE")) {
            final Expression elsePart = readExpression();
            read("END");
            readIf("CASE");
            return elsePart;
        }
        readIf("WHEN");
        Expression when = readExpression();
        if (left != null) {
            when = new Comparison(session, Comparison.EQUAL, left, when);
        }
        read("THEN");
        final Expression then = readExpression();
        final Expression elsePart = readWhen(left);
        final Function function = Function.getFunction(session.getDatabase(), "CASEWHEN");
        function.setParameter(0, when);
        function.setParameter(1, then);
        function.setParameter(2, elsePart);
        function.doneWithParameters();
        return function;
    }

    private int getPositiveInt() throws SQLException {

        final int v = getInt();
        if (v < 0) { throw Message.getInvalidValueException("" + v, "positive integer"); }
        return v;
    }

    private int getInt() throws SQLException {

        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        }
        else if (currentTokenType == PLUS) {
            read();
        }
        if (currentTokenType != VALUE || currentValue.getType() != Value.INT) { throw Message.getSyntaxError(sqlCommand, parseIndex, "integer"); }
        final int i = currentValue.getInt();
        read();
        return minus ? -i : i;
    }

    private long readLong() throws SQLException {

        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        }
        if (currentTokenType != VALUE || currentValue.getType() != Value.INT && currentValue.getType() != Value.DECIMAL) { throw Message.getSyntaxError(sqlCommand, parseIndex, "long"); }
        final long i = currentValue.getLong();
        read();
        return minus ? -i : i;
    }

    private boolean readBooleanSetting() throws SQLException {

        if (currentTokenType == VALUE) {
            final boolean result = currentValue.getBoolean().booleanValue();
            read();
            return result;
        }
        if (readIf("TRUE") || readIf("ON")) {
            return true;
        }
        else if (readIf("FALSE") || readIf("OFF")) {
            return false;
        }
        else {
            throw getSyntaxError();
        }
    }

    private String readString() throws SQLException {

        final Expression expr = readExpression().optimize(session);
        if (!(expr instanceof ValueExpression)) { throw Message.getSyntaxError(sqlCommand, parseIndex, "string"); }
        final String s = expr.getValue(session).getString();
        return s;
    }

    private String readIdentifierWithSchema(final String defaultSchemaName) throws SQLException {

        if (currentTokenType != IDENTIFIER) { throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier"); }
        String s = currentToken;
        read();
        schemaName = defaultSchemaName;
        if (readIf(".")) {
            schemaName = s;
            if (currentTokenType != IDENTIFIER) { throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier"); }
            s = currentToken;
            read();
        }
        if (".".equals(currentToken)) {
            if (schemaName.equalsIgnoreCase(database.getShortName())) {
                read(".");
                schemaName = s;
                if (currentTokenType != IDENTIFIER) { throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier"); }
                s = currentToken;
                read();
            }
        }
        return s;
    }

    private String readIdentifierWithSchema() throws SQLException {

        return readIdentifierWithSchema(session.getCurrentSchemaName());
    }

    private String readAliasIdentifier() throws SQLException {

        return readColumnIdentifier();
    }

    private String readUniqueIdentifier() throws SQLException {

        return readColumnIdentifier();
    }

    private String readColumnIdentifier() throws SQLException {

        if (currentTokenType != IDENTIFIER) { throw Message.getSyntaxError(sqlCommand, parseIndex, "identifier"); }
        final String s = currentToken;
        read();
        return s;
    }

    private void read(final String expected) throws SQLException {

        if (!expected.equals(currentToken) || currentTokenQuoted) { throw Message.getSyntaxError(sqlCommand, parseIndex, expected); }
        read();
    }

    private boolean readIf(final String token) throws SQLException {

        if (token.equals(currentToken) && !currentTokenQuoted) {
            read();
            return true;
        }
        addExpected(token);
        return false;
    }

    private boolean isToken(final String token) {

        final boolean result = token.equals(currentToken) && !currentTokenQuoted;
        if (result) { return true; }
        addExpected(token);
        return false;
    }

    private void addExpected(final String token) {

        if (expectedList != null) {
            expectedList.add(token);
        }
    }

    private void read() throws SQLException {

        currentTokenQuoted = false;
        if (expectedList != null) {
            expectedList.clear();
        }
        final int[] types = characterTypes;
        lastParseIndex = parseIndex;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {
            type = types[++i];
        }
        int start = i;
        final char[] chars = sqlCommandChars;
        char c = chars[i++];
        currentToken = "";
        switch (type) {
            case CHAR_NAME:
                while (true) {
                    type = types[i];
                    if (type != CHAR_NAME && type != CHAR_VALUE) {
                        break;
                    }
                    i++;
                }
                currentToken = StringCache.getNew(sqlCommand.substring(start, i));
                currentTokenType = getTokenType(currentToken);
                parseIndex = i;
                return;
            case CHAR_QUOTED: {
                String result = null;
                while (true) {
                    for (final int begin = i;; i++) {
                        if (chars[i] == '\"') {
                            if (result == null) {
                                result = sqlCommand.substring(begin, i);
                            }
                            else {
                                result += sqlCommand.substring(begin - 1, i);
                            }
                            break;
                        }
                    }
                    if (chars[++i] != '\"') {
                        break;
                    }
                    i++;
                }
                currentToken = StringCache.getNew(result);
                parseIndex = i;
                currentTokenQuoted = true;
                currentTokenType = IDENTIFIER;
                return;
            }
            case CHAR_SPECIAL_2:
                if (types[i] == CHAR_SPECIAL_2) {
                    i++;
                }
                currentToken = sqlCommand.substring(start, i);
                currentTokenType = getSpecialType(currentToken);
                parseIndex = i;
                return;
            case CHAR_SPECIAL_1:
                currentToken = sqlCommand.substring(start, i);
                currentTokenType = getSpecialType(currentToken);
                parseIndex = i;
                return;
            case CHAR_VALUE:
                if (c == '0' && chars[i] == 'X') {
                    // hex number
                    long number = 0;
                    start += 2;
                    i++;
                    while (true) {
                        c = chars[i];
                        if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
                            checkLiterals(false);
                            currentValue = ValueInt.get((int) number);
                            currentTokenType = VALUE;
                            currentToken = "0";
                            parseIndex = i;
                            return;
                        }
                        number = (number << 4) + c - (c >= 'A' ? 'A' - 0xa : '0');
                        if (number > Integer.MAX_VALUE) {
                            readHexDecimal(start, i);
                            return;
                        }
                        i++;
                    }
                }
                long number = c - '0';
                while (true) {
                    c = chars[i];
                    if (c < '0' || c > '9') {
                        if (c == '.') {
                            readDecimal(start, i);
                            break;
                        }
                        if (c == 'E') {
                            readDecimal(start, i);
                            break;
                        }
                        checkLiterals(false);
                        currentValue = ValueInt.get((int) number);
                        currentTokenType = VALUE;
                        currentToken = "0";
                        parseIndex = i;
                        break;
                    }
                    number = number * 10 + (c - '0');
                    if (number > Integer.MAX_VALUE) {
                        readDecimal(start, i);
                        break;
                    }
                    i++;
                }
                return;
            case CHAR_DECIMAL:
                if (types[i] != CHAR_VALUE) {
                    currentTokenType = KEYWORD;
                    currentToken = ".";
                    parseIndex = i;
                    return;
                }
                readDecimal(i - 1, i);
                return;
            case CHAR_STRING: {
                String result = null;
                while (true) {
                    for (final int begin = i;; i++) {
                        if (chars[i] == '\'') {
                            if (result == null) {
                                result = sqlCommand.substring(begin, i);
                            }
                            else {
                                result += sqlCommand.substring(begin - 1, i);
                            }
                            break;
                        }
                    }
                    if (chars[++i] != '\'') {
                        break;
                    }
                    i++;
                }
                currentToken = "'";
                checkLiterals(true);
                currentValue = ValueString.get(StringCache.getNew(result));
                parseIndex = i;
                currentTokenType = VALUE;
                return;
            }
            case CHAR_DOLLAR_QUOTED_STRING: {
                String result = null;
                final int begin = i - 1;
                while (types[i] == CHAR_DOLLAR_QUOTED_STRING) {
                    i++;
                }
                result = sqlCommand.substring(begin, i);
                currentToken = "'";
                checkLiterals(true);
                currentValue = ValueString.get(StringCache.getNew(result));
                parseIndex = i;
                currentTokenType = VALUE;
                return;
            }
            case CHAR_END:
                currentToken = "";
                currentTokenType = END;
                parseIndex = i;
                return;
            default:
                throw getSyntaxError();
        }
    }

    private void checkLiterals(final boolean text) throws SQLException {

        if (!session.getAllowLiterals()) {
            final int allowed = database.getAllowLiterals();
            if (allowed == Constants.ALLOW_LITERALS_NONE || text && allowed != Constants.ALLOW_LITERALS_ALL) { throw Message.getSQLException(ErrorCode.LITERALS_ARE_NOT_ALLOWED); }
        }
    }

    private void readHexDecimal(final int start, int i) throws SQLException {

        final char[] chars = sqlCommandChars;
        char c;
        do {
            c = chars[++i];
        }
        while (c >= '0' && c <= '9' || c >= 'A' && c <= 'F');
        parseIndex = i;
        final String sub = sqlCommand.substring(start, i);
        final BigDecimal bd = new BigDecimal(new BigInteger(sub, 16));
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    private void readDecimal(final int start, int i) throws SQLException {

        final char[] chars = sqlCommandChars;
        final int[] types = characterTypes;
        // go until the first non-number
        while (true) {
            final int t = types[i];
            if (t != CHAR_DECIMAL && t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        if (chars[i] == 'E') {
            i++;
            if (chars[i] == '+' || chars[i] == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) { throw getSyntaxError(); }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        final String sub = sqlCommand.substring(start, i);
        BigDecimal bd;
        try {
            bd = new BigDecimal(sub);
        }
        catch (final NumberFormatException e) {
            throw Message.getSQLException(ErrorCode.DATA_CONVERSION_ERROR_1, new String[]{sub}, e);
        }
        checkLiterals(false);
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    public Session getSession() {

        return session;
    }

    private void initialize(String sql) throws SQLException {

        if (sql == null) {
            sql = "";
        }
        originalSQL = sql;
        sqlCommand = sql;
        int len = sql.length() + 1;
        final char[] command = new char[len];
        final int[] types = new int[len];
        len--;
        sql.getChars(0, len, command, 0);
        boolean changed = false;
        command[len] = ' ';
        int startLoop = 0;
        int lastType = 0;
        // TODO optimization in parser: could remember the length of each token
        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
                case '/':
                    if (command[i + 1] == '*') {
                        // block comment
                        changed = true;
                        command[i] = ' ';
                        command[i + 1] = ' ';
                        startLoop = i;
                        i += 2;
                        checkRunOver(i, len, startLoop);
                        while (command[i] != '*' || command[i + 1] != '/') {
                            command[i++] = ' ';
                            checkRunOver(i, len, startLoop);
                        }
                        command[i] = ' ';
                        command[i + 1] = ' ';
                        i++;
                    }
                    else if (command[i + 1] == '/') {
                        // single line comment
                        changed = true;
                        startLoop = i;
                        while (true) {
                            c = command[i];
                            if (c == '\n' || c == '\r' || i >= len - 1) {
                                break;
                            }
                            command[i++] = ' ';
                            checkRunOver(i, len, startLoop);
                        }
                    }
                    else {
                        type = CHAR_SPECIAL_1;
                    }
                    break;
                case '-':
                    if (command[i + 1] == '-') {
                        // single line comment
                        changed = true;
                        startLoop = i;
                        while (true) {
                            c = command[i];
                            if (c == '\n' || c == '\r' || i >= len - 1) {
                                break;
                            }
                            command[i++] = ' ';
                            checkRunOver(i, len, startLoop);
                        }
                    }
                    else {
                        type = CHAR_SPECIAL_1;
                    }
                    break;
                case '$':
                    if (SysProperties.DOLLAR_QUOTING && command[i + 1] == '$' && (i == 0 || command[i - 1] <= ' ')) {
                        // dollar quoted string
                        changed = true;
                        command[i] = ' ';
                        command[i + 1] = ' ';
                        startLoop = i;
                        i += 2;
                        checkRunOver(i, len, startLoop);
                        while (command[i] != '$' || command[i + 1] != '$') {
                            types[i++] = CHAR_DOLLAR_QUOTED_STRING;
                            checkRunOver(i, len, startLoop);
                        }
                        command[i] = ' ';
                        command[i + 1] = ' ';
                        i++;
                    }
                    else {
                        if (lastType == CHAR_NAME) {
                            // $ inside an identifier is supported
                            type = CHAR_NAME;
                        }
                        else {
                            // but not at the start, to support PostgreSQL $1
                            type = CHAR_SPECIAL_1;
                        }
                    }
                    break;
                case '(':
                case ')':
                case '{':
                case '}':
                case '*':
                case ',':
                case ';':
                case '+':
                case '%':
                case '?':
                case '@':
                case ']':
                    type = CHAR_SPECIAL_1;
                    break;
                case '!':
                case '<':
                case '>':
                case '|':
                case '=':
                case ':':
                case '~':
                    type = CHAR_SPECIAL_2;
                    break;
                case '.':
                    type = CHAR_DECIMAL;
                    break;
                case '\'':
                    type = types[i] = CHAR_STRING;
                    startLoop = i;
                    while (command[++i] != '\'') {
                        checkRunOver(i, len, startLoop);
                    }
                    break;
                case '[':
                    if (database.getMode().squareBracketQuotedNames) {
                        // SQL Server alias for "
                        command[i] = '"';
                        changed = true;
                        type = types[i] = CHAR_QUOTED;
                        startLoop = i;
                        while (command[++i] != ']') {
                            checkRunOver(i, len, startLoop);
                        }
                        command[i] = '"';
                    }
                    else {
                        type = CHAR_SPECIAL_1;
                    }
                    break;
                case '`':
                    // MySQL alias for ", but not case sensitive
                    command[i] = '"';
                    changed = true;
                    type = types[i] = CHAR_QUOTED;
                    startLoop = i;
                    while (command[++i] != '`') {
                        checkRunOver(i, len, startLoop);
                        c = command[i];
                        command[i] = Character.toUpperCase(c);
                    }
                    command[i] = '"';
                    break;
                case '\"':
                    type = types[i] = CHAR_QUOTED;
                    startLoop = i;
                    while (command[++i] != '\"') {
                        checkRunOver(i, len, startLoop);
                    }
                    break;
                case '_':
                    type = CHAR_NAME;
                    break;
                default:
                    if (c >= 'a' && c <= 'z') {
                        command[i] = (char) (c - ('a' - 'A'));
                        changed = true;
                        type = CHAR_NAME;
                    }
                    else if (c >= 'A' && c <= 'Z') {
                        type = CHAR_NAME;
                    }
                    else if (c >= '0' && c <= '9') {
                        type = CHAR_VALUE;
                    }
                    else {
                        if (Character.isJavaIdentifierPart(c)) {
                            type = CHAR_NAME;
                            final char u = Character.toUpperCase(c);
                            if (u != c) {
                                command[i] = u;
                                changed = true;
                            }
                        }
                    }
            }
            types[i] = (byte) type;
            lastType = type;
        }
        sqlCommandChars = command;
        types[len] = CHAR_END;
        characterTypes = types;
        if (changed) {
            sqlCommand = new String(command);
        }
        parseIndex = 0;
    }

    private void checkRunOver(final int i, final int len, final int startLoop) throws SQLException {

        if (i >= len) {
            parseIndex = startLoop;
            throw getSyntaxError();
        }
    }

    private int getSpecialType(final String s) throws SQLException {

        final char c0 = s.charAt(0);
        if (s.length() == 1) {
            switch (c0) {
                case '?':
                case '$':
                    return PARAMETER;
                case '@':
                    return AT;
                case '+':
                    return PLUS;
                case '-':
                    return MINUS;
                case '{':
                case '}':
                case '*':
                case '/':
                case ';':
                case ',':
                case ':':
                case '[':
                case ']':
                case '~':
                    return KEYWORD;
                case '(':
                    return OPEN;
                case ')':
                    return CLOSE;
                case '<':
                    return SMALLER;
                case '>':
                    return BIGGER;
                case '=':
                    return EQUAL;
                default:
                    break;
            }
        }
        else if (s.length() == 2) {
            switch (c0) {
                case ':':
                    if ("::".equals(s)) {
                        return KEYWORD;
                    }
                    else if (":=".equals(s)) { return KEYWORD; }
                    break;
                case '>':
                    if (">=".equals(s)) { return BIGGER_EQUAL; }
                    break;
                case '<':
                    if ("<=".equals(s)) {
                        return SMALLER_EQUAL;
                    }
                    else if ("<>".equals(s)) { return NOT_EQUAL; }
                    break;
                case '!':
                    if ("!=".equals(s)) {
                        return NOT_EQUAL;
                    }
                    else if ("!~".equals(s)) { return KEYWORD; }
                    break;
                case '|':
                    if ("||".equals(s)) { return STRING_CONCAT; }
                    break;
            }
        }
        throw getSyntaxError();
    }

    private int getTokenType(final String s) throws SQLException {

        final int len = s.length();
        if (len == 0) { throw getSyntaxError(); }
        return getSaveTokenType(s, database.getMode().supportOffsetFetch);
    }

    /**
     * Checks if this string is a SQL keyword.
     * 
     * @param s
     *            the token to check
     * @param supportOffsetFetch
     *            if OFFSET and FETCH are keywords
     * @return true if it is a keyword
     */
    public static boolean isKeyword(final String s, final boolean supportOffsetFetch) {

        if (s == null || s.length() == 0) { return false; }
        return getSaveTokenType(s, supportOffsetFetch) != IDENTIFIER;
    }

    private static int getSaveTokenType(final String s, final boolean supportOffsetFetch) {

        switch (s.charAt(0)) {
            case 'C':
                if (s.equals("CURRENT_TIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                }
                else if (s.equals("CURRENT_TIME")) {
                    return CURRENT_TIME;
                }
                else if (s.equals("CURRENT_DATE")) { return CURRENT_DATE; }
                return getKeywordOrIdentifier(s, "CROSS", KEYWORD);
            case 'D':
                return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
            case 'E':
                if ("EXCEPT".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
            case 'F':
                if ("FROM".equals(s)) {
                    return KEYWORD;
                }
                else if ("FOR".equals(s)) {
                    return KEYWORD;
                }
                else if ("FULL".equals(s)) {
                    return KEYWORD;
                }
                else if (supportOffsetFetch && "FETCH".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "FALSE", FALSE);
            case 'G':
                return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
            case 'H':
                return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
            case 'I':
                if ("INNER".equals(s)) {
                    return KEYWORD;
                }
                else if ("INTERSECT".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "IS", KEYWORD);
            case 'J':
                return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
            case 'L':
                if ("LIMIT".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
            case 'M':
                return getKeywordOrIdentifier(s, "MINUS", KEYWORD);
            case 'N':
                if ("NOT".equals(s)) {
                    return KEYWORD;
                }
                else if ("NATURAL".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "NULL", NULL);
            case 'O':
                if ("ON".equals(s)) {
                    return KEYWORD;
                }
                else if (supportOffsetFetch && "OFFSET".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
            case 'P':
                return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
            case 'R':
                return getKeywordOrIdentifier(s, "ROWNUM", ROWNUM);
            case 'S':
                if (s.equals("SYSTIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                }
                else if (s.equals("SYSTIME")) {
                    return CURRENT_TIME;
                }
                else if (s.equals("SYSDATE")) { return CURRENT_TIMESTAMP; }
                return getKeywordOrIdentifier(s, "SELECT", KEYWORD);
            case 'T':
                if ("TODAY".equals(s)) { return CURRENT_DATE; }
                return getKeywordOrIdentifier(s, "TRUE", TRUE);
            case 'U':
                if ("UNIQUE".equals(s)) { return KEYWORD; }
                return getKeywordOrIdentifier(s, "UNION", KEYWORD);
            case 'W':
                return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
            default:
                return IDENTIFIER;
        }
    }

    private static int getKeywordOrIdentifier(final String s1, final String s2, final int keywordType) {

        if (s1.equals(s2)) { return keywordType; }
        return IDENTIFIER;
    }

    private Column parseColumnForTable(final String columnName) throws SQLException {

        Column column;
        final boolean isIdentity = false;
        if (readIf("IDENTITY") || readIf("SERIAL")) {
            column = new Column(columnName, Value.LONG);
            column.setOriginalSQL("IDENTITY");
            parseAutoIncrement(column);
            column.setPrimaryKey(true);
        }
        else {
            column = parseColumn(columnName);
        }
        if (readIf("NOT")) {
            read("NULL");
            column.setNullable(false);
        }
        else {
            readIf("NULL");
            column.setNullable(true);
        }
        if (readIf("AS")) {
            if (isIdentity) {
                getSyntaxError();
            }
            final Expression expr = readExpression();
            column.setComputedExpression(expr);
        }
        else if (readIf("DEFAULT")) {
            final Expression defaultExpression = readExpression();
            column.setDefaultExpression(session, defaultExpression);
        }
        else if (readIf("GENERATED")) {
            if (!readIf("ALWAYS")) {
                read("BY");
                read("DEFAULT");
            }
            read("AS");
            read("IDENTITY");
            long start = 1, increment = 1;
            if (readIf("(")) {
                read("START");
                readIf("WITH");
                start = readLong();
                readIf(",");
                if (readIf("INCREMENT")) {
                    readIf("BY");
                    increment = readLong();
                }
                read(")");
            }
            column.setPrimaryKey(true);
            column.setAutoIncrement(true, start, increment);
        }
        if (readIf("NOT")) {
            read("NULL");
            column.setNullable(false);
        }
        else {
            readIf("NULL");
        }
        if (readIf("AUTO_INCREMENT")) {
            parseAutoIncrement(column);
            if (readIf("NOT")) {
                read("NULL");
            }
        }
        else if (readIf("IDENTITY")) {
            parseAutoIncrement(column);
            column.setPrimaryKey(true);
            if (readIf("NOT")) {
                read("NULL");
            }
        }
        if (readIf("NULL_TO_DEFAULT")) {
            column.setConvertNullToDefault(true);
        }
        if (readIf("SEQUENCE")) {
            final Sequence sequence = readSequence();
            column.setSequence(sequence);
        }
        if (readIf("SELECTIVITY")) {
            final int value = getPositiveInt();
            column.setSelectivity(value);
        }
        final String comment = readCommentIf();
        if (comment != null) {
            column.setComment(comment);
        }
        return column;
    }

    private void parseAutoIncrement(final Column column) throws SQLException {

        long start = 1, increment = 1;
        if (readIf("(")) {
            start = readLong();
            if (readIf(",")) {
                increment = readLong();
            }
            read(")");
        }
        column.setAutoIncrement(true, start, increment);
    }

    private String readCommentIf() throws SQLException {

        if (readIf("COMMENT")) {
            readIf("IS");
            return readString();
        }
        return null;
    }

    private Column parseColumn(final String columnName) throws SQLException {

        String original = currentToken;
        boolean regular = false;
        if (readIf("LONG")) {
            if (readIf("RAW")) {
                original += " RAW";
            }
        }
        else if (readIf("DOUBLE")) {
            if (readIf("PRECISION")) {
                original += " PRECISION";
            }
        }
        else if (readIf("CHARACTER")) {
            if (readIf("VARYING")) {
                original += " VARYING";
            }
        }
        else {
            regular = true;
        }
        long precision = -1;
        int displaySize = -1;
        int scale = -1;
        String comment = null;
        Column templateColumn = null;
        DataType dataType;
        final UserDataType userDataType = database.findUserDataType(original);
        if (userDataType != null) {
            templateColumn = userDataType.getColumn();
            dataType = DataType.getDataType(templateColumn.getType());
            comment = templateColumn.getComment();
            original = templateColumn.getOriginalSQL();
            precision = templateColumn.getPrecision();
            displaySize = templateColumn.getDisplaySize();
            scale = templateColumn.getScale();
        }
        else {
            dataType = DataType.getTypeByName(original);
            if (dataType == null) { throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, currentToken); }
        }
        if (database.getIgnoreCase() && dataType.type == Value.STRING && !"VARCHAR_CASESENSITIVE".equals(original)) {
            original = "VARCHAR_IGNORECASE";
            dataType = DataType.getTypeByName(original);
        }
        if (dataType.type == Value.NULL) {
            // We do support NULL in the database meta data,
            // but not actually when creating tables.
            throw Message.getSQLException(ErrorCode.UNKNOWN_DATA_TYPE_1, original);
        }
        if (regular) {
            read();
        }
        precision = precision == -1 ? dataType.defaultPrecision : precision;
        displaySize = displaySize == -1 ? dataType.defaultDisplaySize : displaySize;
        scale = scale == -1 ? dataType.defaultScale : scale;
        if (dataType.supportsPrecision || dataType.supportsScale) {
            if (readIf("(")) {
                long p = readLong();
                if (readIf("K")) {
                    p *= 1024;
                }
                else if (readIf("M")) {
                    p *= 1024 * 1024;
                }
                else if (readIf("G")) {
                    p *= 1024 * 1024 * 1024;
                }
                if (p > Long.MAX_VALUE) {
                    p = Long.MAX_VALUE;
                }
                original += "(" + p;
                // Oracle syntax
                readIf("CHAR");
                if (dataType.supportsScale) {
                    if (readIf(",")) {
                        scale = getInt();
                        original += ", " + scale;
                    }
                    else {
                        // special case: TIMESTAMP(5) actually means
                        // TIMESTAMP(23, 5)
                        if (dataType.type == Value.TIMESTAMP) {
                            scale = MathUtils.convertLongToInt(p);
                            p = precision;
                        }
                        else {
                            scale = 0;
                        }
                    }
                }
                precision = p;
                displaySize = MathUtils.convertLongToInt(precision);
                original += ")";
                read(")");
            }
        }
        else if (readIf("(")) {
            // Support for MySQL: INT(11), MEDIUMINT(8) and so on.
            // Just ignore the precision.
            getPositiveInt();
            read(")");
        }
        if (readIf("FOR")) {
            read("BIT");
            read("DATA");
            if (dataType.type == Value.STRING) {
                dataType = DataType.getTypeByName("BINARY");
            }
        }
        // MySQL compatibility
        readIf("UNSIGNED");
        final int type = dataType.type;
        final Column column = new Column(columnName, type, precision, scale, displaySize);
        if (templateColumn != null) {
            column.setNullable(templateColumn.getNullable());
            column.setDefaultExpression(session, templateColumn.getDefaultExpression());
            final int selectivity = templateColumn.getSelectivity();
            if (selectivity != Constants.SELECTIVITY_DEFAULT) {
                column.setSelectivity(selectivity);
            }
            final Expression checkConstraint = templateColumn.getCheckConstraint(session, columnName);
            if (checkConstraint != null) {
                column.addCheckConstraint(session, checkConstraint);
            }
        }
        column.setComment(comment);
        column.setOriginalSQL(original);
        return column;
    }

    private Prepared parseCreate() throws SQLException {

        final boolean force = readIf("FORCE");
        if (readIf("LOCAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) { return parseCreateLinkedTable(true, false, force); }
            read("TABLE");
            return parseCreateTable(true, false, false);
        }
        else if (readIf("GLOBAL")) {
            read("TEMPORARY");
            if (readIf("LINKED")) { return parseCreateLinkedTable(true, true, force); }
            read("TABLE");
            return parseCreateTable(true, true, false);
        }
        else if (readIf("TEMP") || readIf("TEMPORARY")) {
            if (readIf("LINKED")) { return parseCreateLinkedTable(true, true, force); }
            read("TABLE");
            return parseCreateTable(true, true, false);
        }
        else if (readIf("MEMORY")) {
            read("TABLE");
            return parseCreateTable(false, false, false);
        }
        else if (readIf("LINKED")) {
            return parseCreateLinkedTable(false, false, force);
        }
        else if (readIf("CACHED")) {
            read("TABLE");
            return parseCreateTable(false, false, true);
        }
        else if (readIf("TABLE")) {
            int defaultMode;
            final Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_TABLE_TYPE));
            defaultMode = setting == null ? Constants.DEFAULT_TABLE_TYPE : setting.getIntValue();
            return parseCreateTable(false, false, defaultMode == Table.TYPE_CACHED);
        }
        else if (readIf("EMPTY")) {
            read("REPLICA");
            int defaultMode;
            final Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_TABLE_TYPE));
            defaultMode = setting == null ? Constants.DEFAULT_TABLE_TYPE : setting.getIntValue();
            try {
                return parseCreateReplica(false, false, defaultMode == Table.TYPE_CACHED, true);
            }
            catch (final RPCException e) {
                e.printStackTrace();
                return null;
            }
        }
        else if (readIf("REPLICA")) {
            int defaultMode;
            final Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_TABLE_TYPE));
            defaultMode = setting == null ? Constants.DEFAULT_TABLE_TYPE : setting.getIntValue();
            try {
                return parseCreateReplica(false, false, defaultMode == Table.TYPE_CACHED, false);
            }
            catch (final RPCException e) {
                e.printStackTrace();
                return null;
            }
        }
        else if (readIf("VIEW")) {
            return parseCreateView(force);
        }
        else if (readIf("ALIAS")) {
            return parseCreateFunctionAlias(force);
        }
        else if (readIf("SEQUENCE")) {
            return parseCreateSequence();
        }
        else if (readIf("USER")) {
            return parseCreateUser();
        }
        else if (readIf("TRIGGER")) {
            return parseCreateTrigger(force);
        }
        else if (readIf("ROLE")) {
            return parseCreateRole();
        }
        else if (readIf("SCHEMA")) {
            return parseCreateSchema();
        }
        else if (readIf("CONSTANT")) {
            return parseCreateConstant();
        }
        else if (readIf("DOMAIN")) {
            return parseCreateUserDataType();
        }
        else if (readIf("TYPE")) {
            return parseCreateUserDataType();
        }
        else if (readIf("DATATYPE")) {
            return parseCreateUserDataType();
        }
        else if (readIf("AGGREGATE")) {
            return parseCreateAggregate(force);
        }
        else {
            boolean hash = false, primaryKey = false, unique = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if (readIf("PRIMARY")) {
                read("KEY");
                if (readIf("HASH")) {
                    hash = true;
                }
                primaryKey = true;
            }
            else {
                if (readIf("UNIQUE")) {
                    unique = true;
                    if (readIf("HASH")) {
                        hash = true;
                    }
                }
                if (readIf("INDEX")) {
                    if (!isToken("ON")) {
                        ifNotExists = readIfNoExists();
                        indexName = readIdentifierWithSchema(null);
                        oldSchema = getSchema();
                    }
                }
                else {
                    throw getSyntaxError();
                }
            }
            read("ON");
            final String tableName = readIdentifierWithSchema();
            checkSchema(oldSchema);
            final CreateIndex command = new CreateIndex(session, getSchema());
            command.setIfNotExists(ifNotExists);
            command.setHash(hash);
            command.setPrimaryKey(primaryKey);
            command.setTableName(tableName);
            command.setUnique(unique);
            command.setIndexName(indexName);
            command.setComment(readCommentIf());
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            return command;
        }
    }

    private Prepared parseNew() throws SQLException {

        return null;
    }

    private boolean addRoleOrRight(final GrantRevoke command) throws SQLException {

        if (readIf("SELECT")) {
            command.addRight(Right.SELECT);
            return false;
        }
        else if (readIf("DELETE")) {
            command.addRight(Right.DELETE);
            return false;
        }
        else if (readIf("INSERT")) {
            command.addRight(Right.INSERT);
            return false;
        }
        else if (readIf("UPDATE")) {
            command.addRight(Right.UPDATE);
            return false;
        }
        else if (readIf("ALL")) {
            command.addRight(Right.ALL);
            return false;
        }
        else if (readIf("CONNECT")) {
            // ignore this right
            return false;
        }
        else if (readIf("RESOURCE")) {
            // ignore this right
            return false;
        }
        else {
            command.addRoleName(readUniqueIdentifier());
            return true;
        }
    }

    private GrantRevoke parseGrantRevoke(final int operationType) throws SQLException {

        final GrantRevoke command = new GrantRevoke(session);
        command.setOperationType(operationType);
        final boolean isRoleBased = addRoleOrRight(command);
        while (readIf(",")) {
            final boolean next = addRoleOrRight(command);
            if (next != isRoleBased) { throw Message.getSQLException(ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED); }
        }
        if (!isRoleBased) {
            if (readIf("ON")) {
                do {
                    final Table table = readTableOrView();
                    command.addTable(table);
                }
                while (readIf(","));
            }
        }
        if (operationType == GrantRevoke.GRANT) {
            read("TO");
        }
        else {
            read("FROM");
        }
        command.setGranteeName(readUniqueIdentifier());
        return command;
    }

    private Call parserCall() throws SQLException {

        final Call command = new Call(session, internalQuery);
        currentPrepared = command;
        command.setValue(readExpression());
        return command;
    }

    private CreateRole parseCreateRole() throws SQLException {

        final CreateRole command = new CreateRole(session);
        command.setIfNotExists(readIfNoExists());
        command.setRoleName(readUniqueIdentifier());
        return command;
    }

    private CreateSchema parseCreateSchema() throws SQLException {

        final CreateSchema command = new CreateSchema(session);
        command.setIfNotExists(readIfNoExists());
        command.setSchemaName(readUniqueIdentifier());
        if (readIf("AUTHORIZATION")) {
            command.setAuthorization(readUniqueIdentifier());
        }
        else {
            command.setAuthorization(session.getUser().getName());
        }
        return command;
    }

    private CreateSequence parseCreateSequence() throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final String sequenceName = readIdentifierWithSchema();
        final CreateSequence command = new CreateSequence(session, getSchema());
        command.setIfNotExists(ifNotExists);
        command.setSequenceName(sequenceName);
        while (true) {
            if (readIf("START")) {
                readIf("WITH");
                command.setStartWith(readExpression());
            }
            else if (readIf("INCREMENT")) {
                readIf("BY");
                command.setIncrement(readExpression());
            }
            else if (readIf("CACHE")) {
                command.setCacheSize(readExpression());
            }
            else if (readIf("BELONGS_TO_TABLE")) {
                command.setBelongsToTable(true);
            }
            else {
                break;
            }
        }
        return command;
    }

    private boolean readIfNoExists() throws SQLException {

        if (readIf("IF")) {
            read("NOT");
            read("EXISTS");
            return true;
        }
        return false;
    }

    private boolean readUpdateData() throws SQLException {

        if (readIf("UPDATE")) {
            read("DATA");
            return true;
        }
        return false;
    }

    private CreateConstant parseCreateConstant() throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final String constantName = readIdentifierWithSchema();
        final Schema schema = getSchema();
        if (isKeyword(constantName, false)) { throw Message.getSQLException(ErrorCode.CONSTANT_ALREADY_EXISTS_1, constantName); }
        read("VALUE");
        final Expression expr = readExpression();
        final CreateConstant command = new CreateConstant(session, schema);
        command.setConstantName(constantName);
        command.setExpression(expr);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateAggregate parseCreateAggregate(final boolean force) throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final CreateAggregate command = new CreateAggregate(session);
        command.setForce(force);
        final String name = readUniqueIdentifier();
        if (isKeyword(name, false) || Function.getFunction(database, name) != null || Aggregate.getAggregateType(name) >= 0) { throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name); }
        command.setName(name);
        command.setIfNotExists(ifNotExists);
        read("FOR");
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private CreateUserDataType parseCreateUserDataType() throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final CreateUserDataType command = new CreateUserDataType(session);
        command.setTypeName(readUniqueIdentifier());
        read("AS");
        final Column col = parseColumnForTable("VALUE");
        if (readIf("CHECK")) {
            final Expression expr = readExpression();
            col.addCheckConstraint(session, expr);
        }
        col.rename(null);
        command.setColumn(col);
        command.setIfNotExists(ifNotExists);
        return command;
    }

    private CreateTrigger parseCreateTrigger(final boolean force) throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final String triggerName = readIdentifierWithSchema(null);
        final Schema schema = getSchema();
        boolean isBefore;
        if (readIf("BEFORE")) {
            isBefore = true;
        }
        else {
            read("AFTER");
            isBefore = false;
        }
        int typeMask = 0;
        do {
            if (readIf("INSERT")) {
                typeMask |= Trigger.INSERT;
            }
            else if (readIf("UPDATE")) {
                typeMask |= Trigger.UPDATE;
            }
            else if (readIf("DELETE")) {
                typeMask |= Trigger.DELETE;
            }
            else {
                throw getSyntaxError();
            }
        }
        while (readIf(","));
        read("ON");
        final String tableName = readIdentifierWithSchema();
        checkSchema(schema);
        final CreateTrigger command = new CreateTrigger(session, getSchema());
        command.setForce(force);
        command.setTriggerName(triggerName);
        command.setIfNotExists(ifNotExists);
        command.setBefore(isBefore);
        command.setTypeMask(typeMask);
        command.setTableName(tableName);
        if (readIf("FOR")) {
            read("EACH");
            read("ROW");
            command.setRowBased(true);
        }
        else {
            command.setRowBased(false);
        }
        if (readIf("QUEUE")) {
            command.setQueueSize(getPositiveInt());
        }
        command.setNoWait(readIf("NOWAIT"));
        read("CALL");
        command.setTriggerClassName(readUniqueIdentifier());
        return command;
    }

    private CreateUser parseCreateUser() throws SQLException {

        final CreateUser command = new CreateUser(session);
        command.setIfNotExists(readIfNoExists());
        command.setUserName(readUniqueIdentifier());
        command.setComment(readCommentIf());
        if (readIf("PASSWORD")) {
            command.setPassword(readExpression());
        }
        else if (readIf("SALT")) {
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
        }
        else if (readIf("IDENTIFIED")) {
            read("BY");
            // uppercase if not quoted
            command.setPassword(ValueExpression.get(ValueString.get(readColumnIdentifier())));
        }
        else {
            throw getSyntaxError();
        }
        if (readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private CreateFunctionAlias parseCreateFunctionAlias(final boolean force) throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final CreateFunctionAlias command = new CreateFunctionAlias(session);
        command.setForce(force);
        final String name = readUniqueIdentifier();
        if (isKeyword(name, false) || Function.getFunction(database, name) != null || Aggregate.getAggregateType(name) >= 0) { throw Message.getSQLException(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, name); }
        command.setAliasName(name);
        command.setIfNotExists(ifNotExists);
        command.setDeterministic(readIf("DETERMINISTIC"));
        read("FOR");
        command.setJavaClassMethod(readUniqueIdentifier());
        return command;
    }

    private Query parserWith() throws SQLException {

        final String tempViewName = readIdentifierWithSchema();
        final Schema schema = getSchema();
        TableData recursiveTable;
        read("(");
        final String[] cols = parseColumnList();
        final ObjectArray columns = new ObjectArray();
        for (final String col : cols) {
            columns.add(new Column(col, Value.STRING));
        }
        int id = database.allocateObjectId(true, true);
        recursiveTable = schema.createTable(tempViewName, id, columns, false, false, Index.EMPTY_HEAD);
        recursiveTable.setTemporary(true);
        session.addLocalTempTable(recursiveTable);
        final String querySQL = StringCache.getNew(sqlCommand.substring(parseIndex));
        read("AS");
        final Query withQuery = parseSelect();
        withQuery.prepare();
        session.removeLocalTempTable(recursiveTable);
        id = database.allocateObjectId(true, true);
        final TableView view = new TableView(schema, id, tempViewName, querySQL, null, cols, session, true);
        view.setTemporary(true);
        // view.setOnCommitDrop(true);
        session.addLocalTempTable(view);
        final Query query = parseSelect();
        query.prepare();
        query.setPrepareAlways(true);
        // session.removeLocalTempTable(view);
        return query;
    }

    private CreateView parseCreateView(final boolean force) throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final String viewName = readIdentifierWithSchema();
        final CreateView command = new CreateView(session, getSchema());
        prepared = command;
        command.setViewName(viewName);
        command.setIfNotExists(ifNotExists);
        command.setComment(readCommentIf());
        if (readIf("(")) {
            final String[] cols = parseColumnList();
            command.setColumnNames(cols);
        }
        final String select = StringCache.getNew(sqlCommand.substring(parseIndex));
        read("AS");
        try {
            final Query query = parseSelect();
            query.prepare();
            command.setSelect(query);
        }
        catch (final SQLException e) {
            if (force) {
                command.setSelectSQL(select);
            }
            else {
                throw e;
            }
        }
        return command;
    }

    private TransactionCommand parseCheckpoint() throws SQLException {

        TransactionCommand command;
        if (readIf("SYNC")) {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT_SYNC, internalQuery);
        }
        else {
            command = new TransactionCommand(session, TransactionCommand.CHECKPOINT, internalQuery);
        }
        return command;
    }

    private Prepared parseAlter() throws SQLException {

        if (readIf("TABLE")) {
            return parseAlterTable();
        }
        else if (readIf("USER")) {
            return parseAlterUser();
        }
        else if (readIf("INDEX")) {
            return parseAlterIndex();
        }
        else if (readIf("SEQUENCE")) {
            return parseAlterSequence();
        }
        else if (readIf("VIEW")) { return parseAlterView(); }
        throw getSyntaxError();
    }

    private void checkSchema(final Schema old) throws SQLException {

        if (old != null && getSchema() != old) { throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH); }
    }

    private AlterIndexRename parseAlterIndex() throws SQLException {

        final String indexName = readIdentifierWithSchema();
        final Schema old = getSchema();
        final AlterIndexRename command = new AlterIndexRename(session);
        command.setOldIndex(getSchema().getIndex(indexName));
        read("RENAME");
        read("TO");
        final String newName = readIdentifierWithSchema(old.getName());
        checkSchema(old);
        command.setNewName(newName);
        return command;
    }

    private AlterView parseAlterView() throws SQLException {

        final AlterView command = new AlterView(session);
        final String viewName = readIdentifierWithSchema();
        final Table tableView = getSchema().findTableOrView(session, viewName, LocationPreference.NO_PREFERENCE);
        if (!(tableView instanceof TableView)) { throw Message.getSQLException(ErrorCode.VIEW_NOT_FOUND_1, viewName); }
        final TableView view = (TableView) tableView;
        command.setView(view);
        read("RECOMPILE");
        return command;
    }

    private AlterSequence parseAlterSequence() throws SQLException {

        final String sequenceName = readIdentifierWithSchema();
        final Sequence sequence = getSchema().getSequence(sequenceName);
        final AlterSequence command = new AlterSequence(session, sequence.getSchema());
        command.setSequence(sequence);
        if (readIf("RESTART")) {
            read("WITH");
            command.setStartWith(readExpression());
        }
        if (readIf("INCREMENT")) {
            read("BY");
            command.setIncrement(readExpression());
        }
        return command;
    }

    private AlterUser parseAlterUser() throws SQLException {

        final String userName = readUniqueIdentifier();
        if (readIf("SET")) {
            final AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(database.getUser(userName));
            if (readIf("PASSWORD")) {
                command.setPassword(readExpression());
            }
            else if (readIf("SALT")) {
                command.setSalt(readExpression());
                read("HASH");
                command.setHash(readExpression());
            }
            else {
                throw getSyntaxError();
            }
            return command;
        }
        else if (readIf("RENAME")) {
            read("TO");
            final AlterUser command = new AlterUser(session);
            command.setType(AlterUser.RENAME);
            command.setUser(database.getUser(userName));
            final String newName = readUniqueIdentifier();
            command.setNewName(newName);
            return command;
        }
        else if (readIf("ADMIN")) {
            final AlterUser command = new AlterUser(session);
            command.setType(AlterUser.ADMIN);
            final User user = database.getUser(userName);
            command.setUser(user);
            if (readIf("TRUE")) {
                command.setAdmin(true);
            }
            else if (readIf("FALSE")) {
                command.setAdmin(false);
            }
            else {
                throw getSyntaxError();
            }
            return command;
        }
        throw getSyntaxError();
    }

    private void readIfEqualOrTo() throws SQLException {

        if (!readIf("=")) {
            readIf("TO");
        }
    }

    private Prepared parseSet() throws SQLException {

        if (readIf("@")) {
            final Set command = new Set(session, SetTypes.VARIABLE, internalQuery);
            command.setString(readAliasIdentifier());
            readIfEqualOrTo();
            command.setExpression(readExpression());
            return command;
        }
        else if (readIf("AUTOCOMMIT")) {
            readIfEqualOrTo();
            final boolean value = readBooleanSetting();
            final int setting = value ? TransactionCommand.AUTOCOMMIT_TRUE : TransactionCommand.AUTOCOMMIT_FALSE;
            return new TransactionCommand(session, setting, internalQuery);
        }
        else if (readIf("MVCC")) {
            readIfEqualOrTo();
            final boolean value = readBooleanSetting();
            final Set command = new Set(session, SetTypes.MVCC, internalQuery);
            command.setInt(value ? 1 : 0);
            return command;
        }
        else if (readIf("EXCLUSIVE")) {
            readIfEqualOrTo();
            final boolean value = readBooleanSetting();
            final Set command = new Set(session, SetTypes.EXCLUSIVE, internalQuery);
            command.setInt(value ? 1 : 0);
            return command;
        }
        else if (readIf("IGNORECASE")) {
            readIfEqualOrTo();
            final boolean value = readBooleanSetting();
            final Set command = new Set(session, SetTypes.IGNORECASE, internalQuery);
            command.setInt(value ? 1 : 0);
            return command;
        }
        else if (readIf("PASSWORD")) {
            readIfEqualOrTo();
            final AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setPassword(readExpression());
            return command;
        }
        else if (readIf("SALT")) {
            readIfEqualOrTo();
            final AlterUser command = new AlterUser(session);
            command.setType(AlterUser.SET_PASSWORD);
            command.setUser(session.getUser());
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
            return command;
        }
        else if (readIf("MODE")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.MODE, internalQuery);
            command.setString(readAliasIdentifier());
            return command;
        }
        else if (readIf("COMPRESS_LOB")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.COMPRESS_LOB, internalQuery);
            if (currentTokenType == VALUE) {
                command.setString(readString());
            }
            else {
                command.setString(readUniqueIdentifier());
            }
            return command;
        }
        else if (readIf("DATABASE")) {
            readIfEqualOrTo();
            read("COLLATION");
            return parseSetCollation();
        }
        else if (readIf("COLLATION")) {
            readIfEqualOrTo();
            return parseSetCollation();
        }
        else if (readIf("CLUSTER")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.CLUSTER, internalQuery);
            command.setString(readString());
            return command;
        }
        else if (readIf("DATABASE_EVENT_LISTENER")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.DATABASE_EVENT_LISTENER, internalQuery);
            command.setString(readString());
            return command;
        }
        else if (readIf("ALLOW_LITERALS")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.ALLOW_LITERALS, internalQuery);
            if (readIf("NONE")) {
                command.setInt(Constants.ALLOW_LITERALS_NONE);
            }
            else if (readIf("ALL")) {
                command.setInt(Constants.ALLOW_LITERALS_ALL);
            }
            else if (readIf("NUMBERS")) {
                command.setInt(Constants.ALLOW_LITERALS_NUMBERS);
            }
            else {
                command.setInt(getPositiveInt());
            }
            return command;
        }
        else if (readIf("DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.DEFAULT_TABLE_TYPE, internalQuery);
            if (readIf("MEMORY")) {
                command.setInt(Table.TYPE_MEMORY);
            }
            else if (readIf("CACHED")) {
                command.setInt(Table.TYPE_CACHED);
            }
            else {
                command.setInt(getPositiveInt());
            }
            return command;
        }
        else if (readIf("CREATE")) {
            readIfEqualOrTo();
            // Derby compatibility (CREATE=TRUE in the database URL)
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("HSQLDB.DEFAULT_TABLE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("CACHE_TYPE")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("FILE_LOCK")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("DB_CLOSE_ON_EXIT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("ACCESS_MODE_LOG")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("AUTO_SERVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("AUTO_RECONNECT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("ASSERT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("ACCESS_MODE_DATA")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("DATABASE_EVENT_LISTENER_OBJECT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("OPEN_NEW")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("RECOVER")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("SCHEMA")) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.SCHEMA, internalQuery);
            command.setString(readAliasIdentifier());
            return command;
        }
        else if (readIf("DATESTYLE")) {
            // PostgreSQL compatibility
            readIfEqualOrTo();
            if (!readIf("ISO")) {
                final String s = readString();
                if (!s.equals("ISO")) { throw getSyntaxError(); }
            }
            return new NoOperation(session, internalQuery);
        }
        else if (readIf("SEARCH_PATH") || readIf(SetTypes.getTypeName(SetTypes.SCHEMA_SEARCH_PATH))) {
            readIfEqualOrTo();
            final Set command = new Set(session, SetTypes.SCHEMA_SEARCH_PATH, internalQuery);
            final ObjectArray list = new ObjectArray();
            list.add(readAliasIdentifier());
            while (readIf(",")) {
                list.add(readAliasIdentifier());
            }
            final String[] schemaNames = new String[list.size()];
            list.toArray(schemaNames);
            command.setStringArray(schemaNames);
            return command;
        }
        else if (readIf("REPLICATE")) {
            final boolean allowReplication = readBooleanSetting();

            return new SetReplicate(session, allowReplication);
        }
        else {
            if (isToken("LOGSIZE")) {
                // HSQLDB compatibility
                currentToken = SetTypes.getTypeName(SetTypes.MAX_LOG_SIZE);
            }
            final int type = SetTypes.getType(currentToken);
            if (type < 0) { throw getSyntaxError(); }
            read();
            readIfEqualOrTo();
            final Set command = new Set(session, type, internalQuery);
            command.setExpression(readExpression());
            return command;
        }
    }

    private Set parseSetCollation() throws SQLException {

        final Set command = new Set(session, SetTypes.COLLATION, internalQuery);
        final String name = readAliasIdentifier();
        command.setString(name);
        if (name.equals(CompareMode.OFF)) { return command; }
        final Collator coll = CompareMode.getCollator(name);
        if (coll == null) { throw getSyntaxError(); }
        if (readIf("STRENGTH")) {
            if (readIf("PRIMARY")) {
                command.setInt(Collator.PRIMARY);
            }
            else if (readIf("SECONDARY")) {
                command.setInt(Collator.SECONDARY);
            }
            else if (readIf("TERTIARY")) {
                command.setInt(Collator.TERTIARY);
            }
            else if (readIf("IDENTICAL")) {
                command.setInt(Collator.IDENTICAL);
            }
        }
        else {
            command.setInt(coll.getStrength());
        }
        return command;
    }

    private RunScriptCommand parseRunScript() throws SQLException {

        final RunScriptCommand command = new RunScriptCommand(session, internalQuery);
        read("FROM");
        command.setFile(readExpression());
        if (readIf("COMPRESSION")) {
            command.setCompressionAlgorithm(readUniqueIdentifier());
        }
        if (readIf("CIPHER")) {
            command.setCipher(readUniqueIdentifier());
            if (readIf("PASSWORD")) {
                command.setPassword(readString().toCharArray());
            }
        }
        if (readIf("CHARSET")) {
            command.setCharset(readString());
        }
        return command;
    }

    private ScriptCommand parseScript() throws SQLException {

        final ScriptCommand command = new ScriptCommand(session, internalQuery);
        boolean data = true, passwords = true, settings = true, dropTables = false, simple = false;

        if (readIf("TABLE")) {
            command.setTable(readIdentifierWithSchema());
            command.setSchema(getSchema().getName());
        }
        if (readIf("SIMPLE")) {
            simple = true;
        }
        if (readIf("NODATA")) {
            data = false;
        }
        if (readIf("NOPASSWORDS")) {
            passwords = false;
        }
        if (readIf("NOSETTINGS")) {
            settings = false;
        }
        if (readIf("DROP")) {
            dropTables = true;
        }
        if (readIf("BLOCKSIZE")) {
            final long blockSize = readLong();
            command.setLobBlockSize(blockSize);
        }
        command.setData(data);
        command.setPasswords(passwords);
        command.setSettings(settings);
        command.setDrop(dropTables);
        command.setSimple(simple);
        if (readIf("TO")) {
            command.setFile(readExpression());
            if (readIf("COMPRESSION")) {
                command.setCompressionAlgorithm(readUniqueIdentifier());
            }
            if (readIf("CIPHER")) {
                command.setCipher(readUniqueIdentifier());
                if (readIf("PASSWORD")) {
                    command.setPassword(readString().toCharArray());
                }
            }
        }
        return command;
    }

    private Table readTableOrView() throws SQLException {

        return readTableOrView(readIdentifierWithSchema(null), true, LocationPreference.NO_PREFERENCE, true);
    }

    /**
     * H2O-Modified. Added the searchRemote parameter.
     * 
     * @param tableName
     * @param searchRemote
     *            Indicates whether the method will look for a remote copy of the data (true) or if it should just return null (false).
     * @param removeLinkedTable Whether this method will remove an existing linked table. This method can be called recursively through {@link #findViaSystemTable(String, String)}
     * so this prevents a newly created linked table from being continually deleted.
     * @return
     * @throws SQLException
     */
    private Table readTableOrView(final String tableName, final boolean searchRemote, final LocationPreference locale, final boolean removeLinkedTable) throws SQLException {

        String localSchemaName = null;

        if (getSchema() != null) {
            localSchemaName = getSchema().getName();
        }

        /*
         * The database will actually request a lock for a table as necessary in CommandContainer. The following code checks with the Table
         * Manager to find active copies of replicas so that an inactive local copy is not used by mistake. XXX this check happens at the
         * wrong place and introduces a possible race condition if one of the copies that is found becomes unavailable before the query is
         * executed.
         */
        final Queue<DatabaseInstanceWrapper> replicaLocations = new LinkedList<DatabaseInstanceWrapper>(); // set of valid locations for this
        // replica.

        boolean tableFound = false; // whether the table has been found via the System Table.

        if (!tableName.equals("SESSIONS") && !tableName.contains("H2O_") && !database.getLocalSchema().contains(schemaName) && !internalQuery && searchRemote) {
            /*
             * Re. Internal Query: if false it indicates that this is not part of some larger update. Search Remote: only false if this method
             * has already been called, and a linked table has been created. Other evaluations: eliminate local tables.
             */

            if (!database.isConnected()) { throw new SQLException("Can't execute this query because this database instance (" + database.getID() + ") is not connected to a System Table."); }

            if (localSchemaName == null) {
                localSchemaName = Constants.SCHEMA_MAIN;
            }

            final TableInfo tableInfo = new TableInfo(tableName, localSchemaName);

            final ITableManagerRemote tableManager = session.getDatabase().getSystemTableReference().lookup(tableInfo, true);

            if (tableManager != null) {

                tableFound = true;
                final TableProxy tableProxy = getProxyFromTableManager(tableInfo, tableManager);

                if (Settings.CHECK_LOCAL_TABLE_VALIDITY_AT_TABLE_MANAGER) {

                    replicaLocations.addAll(tableProxy.getReplicaLocations().keySet());

                }
                else {
                    replicaLocations.addAll(tableProxy.getReplicaLocations().keySet());

                    //Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Replica locations for table " + tableName + ": " + PrettyPrinter.toString(replicaLocations));
                }

                if (replicaLocations.size() == 0) {
                    ErrorHandling.errorNoEvent("No active replicas for table: " + tableName);
                    throw new SQLException("No active replicas for table: " + tableName);
                }

            }
            else {
                // It might be a view. Continue on and check.
                replicaLocations.add(database.getLocalDatabaseInstanceInWrapper());
            }
        }
        else {

            replicaLocations.add(database.getLocalDatabaseInstanceInWrapper());
            tableFound = true;
        }

        /*
         * If there is an active copy of the database locally, use that. <p>If not, try to access a remote copy.
         */

        if (schemaName != null) {
            final Table table = getSchema().getTableOrView(session, tableName);
            if (table != null) { return table; }
        }
        Table table = database.getSchema(session.getCurrentSchemaName()).findTableOrView(session, tableName, locale);

        table = removeReferenceToLinkedTableIfInvalid(replicaLocations, table, removeLinkedTable);

        /*
         * Return if: the table was found by the ST and is local; the table was found by the ST, and isn't local but a LinkedTable is; or if
         * it wasn't found but this is a view.
         */
        if (isTableAccessible(replicaLocations, tableFound, table)) {
            // TODO Check that this is a table link to a valid location - ( (TableLink) table).getConnection().getUrl();
            return table;
        }

        if (!tableFound) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName); }

        String[] schemaNames = session.getSchemaSearchPath();

        if (schemaNames == null) {
            schemaNames = new String[1];
            schemaNames[0] = session.getCurrentSchemaName();
        }

        for (int i = 0; schemaNames != null && i < schemaNames.length; i++) {
            final Schema s = database.getSchema(schemaNames[i]);
            table = s.findTableOrView(session, tableName, locale);

            if (table != null && (replicaLocations.contains(database.getLocalDatabaseInstanceInWrapper()) || tableFound && table.getTableType().equals(Table.TABLE_LINK)) && (!tableFound && table.getTableType().equals(Table.VIEW) || tableFound)) { return table; }
        }

        /*
         * If the table could be found locally this would have returned by now. There are three possibilities at this point: 1. The table
         * has not been found and doesn't exist. 2. The table has not been found, but currently exists as a linked table. [update: this
         * won't happen as the linked table is given the same name as the table.] 3. The table has not been found, but exists in some remote
         * location.
         */

        if (searchRemote && database.getSystemTableReference().isConnectedToSM() && !(locale == LocationPreference.LOCAL_STRICT)) { // 2 or 3

            try {
                return findViaSystemTable(tableName, localSchemaName);
            }
            catch (final RPCException e) {
                e.printStackTrace();
                throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            } // XXX this might fail if its not the default schema.
        }
        else { // 1
            ErrorHandling.errorNoEvent("Search remote: " + searchRemote + "; isConnected = " + database.getSystemTableReference().isConnectedToSM() + "; locale = " + locale);
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }

    }

    /**
     * Whether a table exists that can be returned to the user.
     * 
     * <p>This will be true if the table is found on the system table and exists somewhere, or if it is not found on the system table but exists locally.
     * In the latter case this will only happen if the 'table' is in fact a view, or if it has only been created as part of the current transaction and has not be committted
     * to the System Table yet.
     * <p>If the table doesn't actually exist it will not exist locally or on the System Table so will throw an exception.
     */
    private boolean isTableAccessible(final Queue<DatabaseInstanceWrapper> replicaLocations, final boolean foundBySystemTable, final Table table) {

        return table != null && table.getName().equals("SESSIONS") || table != null && foundBySystemTable && (replicaLocations.contains(database.getLocalDatabaseInstanceInWrapper()) || table.getTableType().equals(Table.TABLE_LINK)) //is local, or known about locally.
                        || !foundBySystemTable && table != null; //part of the current transaction but hasn't committed yet.
    }

    /**
     * Remove any references in the schema to this table if it is a Linked Table and it points to a location
     * that is no longer a replica.
     * @param replicaLocations      The set of valid replica locations for a particular table
     * @param table                 Local reference for this table.
     * @return
     * @throws SQLException
     */
    private Table removeReferenceToLinkedTableIfInvalid(final Queue<DatabaseInstanceWrapper> replicaLocations, Table table, final boolean removeLinkedTable) throws SQLException {

        final java.util.Set<String> urls = new HashSet<String>();

        for (final DatabaseInstanceWrapper databaseInstanceWrapper : replicaLocations) {
            urls.add(databaseInstanceWrapper.getURL().getURL());
        }

        if (removeLinkedTable && table != null && table instanceof TableLink) {
            final String currentURL = ((TableLink) table).getUrl();
            if (!replicaLocations.contains(currentURL)) {
                final boolean removed = database.getSchema(session.getCurrentSchemaName()).removeLinkedTable(table, urls); //XXX change to pass all linked table references.

                if (removed) {
                    table = null;
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Deleted linked table which pointed to the wrong URL [" + currentURL + "].");
                }

            }
        }

        return table;
    }

    private TableProxy getProxyFromTableManager(final TableInfo tableInfo, ITableManagerRemote tableManager) throws SQLException {

        final ISystemTableReference systemTableReference = session.getDatabase().getSystemTableReference();
        try {
            /*
             * This requests LockType.NONE because it doesn't need a lock for the table at this point - only the location of active
             * instances. It will request a read/write lock at a later point.
             */
            return tableManager.getTableProxy(LockType.NONE, new LockRequest(session));

        }
        catch (final MovedException e) {
            // Query System Table again, bypassing the cache.

            tableManager = systemTableReference.lookup(tableInfo, false);

            try {
                return tableManager.getTableProxy(LockType.NONE, new LockRequest(session));
            }
            catch (final Exception e1) {
                throw new SQLException("Unable to contact Table Manager for " + tableInfo + ":: " + e1.getMessage());
            }
        }
        catch (final Exception e) {
            // Attempt to recreate the table manager in-case it has failed, then try again.
            try {
                ISystemTableMigratable systemTableRemote = systemTableReference.getSystemTable();
                if (systemTableRemote != null) {

                    try {
                        systemTableRemote.recreateTableManager(tableInfo);
                    }
                    catch (final Exception e2) { //try to recover from this error.
                        systemTableRemote = systemTableReference.failureRecovery();
                        systemTableRemote.recreateTableManager(tableInfo);

                    }

                }
                else {
                    throw new SQLException("System table was returned as null.");
                }
            }
            catch (final Exception e2) {
                throw new SQLException("Unable to contact the System Table for " + tableInfo + ":: " + e2.getMessage());
            }

            tableManager = systemTableReference.lookup(tableInfo, false);

            if (tableManager == null) { throw new SQLException("Table Manager was null for table lookup: " + tableInfo); }

            try {
                return tableManager.getTableProxy(LockType.NONE, new LockRequest(session));
            }
            catch (final RPCException e1) {

                // Recreate Table Manager then try again.
                try {
                    tableManager = systemTableReference.getSystemTable().recreateTableManager(tableInfo);
                    return tableManager.getTableProxy(LockType.NONE, new LockRequest(session));
                }
                catch (final Exception e2) {

                    try {
                        ErrorHandling.exceptionError(e2, "Failed to recreate or contact recreated Table Manager at " + ((TableManagerProxy) tableManager).getAddress());
                    }
                    catch (final RPCException e3) {
                        // Only an error that affects the diagnostic message, so we effectively ignore it.
                    }

                    throw new SQLException("Failed to recreate or contact recreated Table Manager: " + e2.getMessage());
                }
            }
            catch (final MovedException e1) {
                throw new SQLException("Table moved: " + e1.getMessage());
            }
        }
    }

    /**
     * H2O. Called when a given table was not found in the local schema. This either indicates that the table doesn't exist anywhere, or
     * that it exists on some remote machine. This method attempts to create a linked table for this table on a remote machine. If not, the
     * usual error is returned.
     * 
     * @param tableName
     *            Name of the table that must be found.
     * @return Information on that table.
     * @throws SQLException
     *             if the table is not found.
     * @throws RPCException
     */
    public Table findViaSystemTable(final String tableName, final String thisSchemaName) throws SQLException, RPCException {

        if (database.getLocalSchema().contains(thisSchemaName)) {
            //This should never happen, but it will if H2O expects a meta-data table to be on this machine when it isn't.
            throw new SQLException("The requested table [" + tableName + "] cannot be found locally, and will not be found in the System Table because it is from a non-replicated schema [" + thisSchemaName + "].");
        }

        /*
         * Attempt to locate the table if it exists remotely.
         */
        String tableLocation = null;

        final TableInfo tableInfo = new TableInfo(tableName, thisSchemaName);
        ITableManagerRemote tableManager = session.getDatabase().getSystemTableReference().lookup(tableInfo, true);

        if (tableManager == null) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableInfo.toString()); }

        Map<DatabaseInstanceWrapper, Integer> replicaLocations = null;
        try {
            replicaLocations = tableManager.getReplicasOnActiveMachines();

        }
        catch (final MovedException e) {
            // Query System Table again, bypassing the cache.

            tableManager = session.getDatabase().getSystemTableReference().lookup(new TableInfo(tableName, thisSchemaName), false);

            try {
                replicaLocations = tableManager.getReplicasOnActiveMachines();
            }
            catch (final MovedException e1) {
                throw new SQLException("Unable to contact Table Manager for " + tableName + ":: " + e1.getMessage());
            }
        }
        catch (final Exception e) {
            // Attempt to recreate the table manager in-case it has failed, then
            // try again.
            try {
                session.getDatabase().getSystemTableReference().getSystemTable().recreateTableManager(tableInfo);
            }
            catch (final MovedException e2) {
                throw new SQLException("Unable to contact the System Table for " + tableName + ":: " + e2.getMessage());
            }

            tableManager = session.getDatabase().getSystemTableReference().lookup(new TableInfo(tableName, thisSchemaName), false);
            try {
                replicaLocations = tableManager.getReplicasOnActiveMachines();
            }
            catch (final MovedException e1) {
                throw new SQLException("Unable to contact Table Manager for " + tableName + ":: " + e1.getMessage());
            }
        }

        /*
         * Must be a different session from that of the executing user transaction, because this must commit in the midst of it all.
         */

        Session sessionToUse = database.getExclusiveSession();
        if (sessionToUse == null) {
            sessionToUse = database.getSystemSession();
        }
        final Parser queryParser = new Parser(sessionToUse, true);

        int result = -1;

        /**
         * XXX code to decide which read replica to query.
         */
        for (final DatabaseInstanceWrapper replicaLocation : replicaLocations.keySet()) {
            tableLocation = replicaLocation.getURL().getURL();

            try {
                if (replicaLocation.getURL().equals(session.getDatabase().getID()) || !replicaLocation.getDatabaseInstance().isAlive()) {
                    continue;
                }
            }
            catch (final Exception e) {
                continue; // remote db isn't accessible.
            }

            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Creating linked table for " + tableInfo.getFullTableName() + " to " + replicaLocation.getURL());

            final String sql = "CREATE LINKED TABLE " + tableInfo.getTableName() + "('org.h2.Driver', '" + tableLocation + "', '" + PersistentSystemTable.USERNAME + "', '" + PersistentSystemTable.PASSWORD + "', '" + tableName + "');";

            try {
                final Command sqlQuery = queryParser.prepareCommand(sql);
                result = sqlQuery.update();
                break;
            }
            catch (final Exception e) {
                e.printStackTrace();
                // Will happen if the connection that that table is broken.
            }
        }

        if (result == 0) {
            // Linked table was successfully added.
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Successfully created linked table '" + tableName + "'. Attempting to access it.");
            return readTableOrView(tableName, false, LocationPreference.PRIMARY, false);
        }
        else {
            throw new SQLException("Couldn't find active copy of table " + tableName + " to connect to.");
        }
    }

    private Sequence readSequence() throws SQLException {

        // same algorithm than readTableOrView
        final String sequenceName = readIdentifierWithSchema(null);
        if (schemaName != null) { return getSchema().getSequence(sequenceName); }
        Sequence sequence = database.getSchema(session.getCurrentSchemaName()).findSequence(sequenceName);
        if (sequence != null) { return sequence; }
        final String[] schemaNames = session.getSchemaSearchPath();
        for (int i = 0; schemaNames != null && i < schemaNames.length; i++) {
            final Schema s = database.getSchema(schemaNames[i]);
            sequence = s.findSequence(sequenceName);
            if (sequence != null) { return sequence; }
        }
        throw Message.getSQLException(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
    }

    private Prepared parseAlterTable() throws SQLException {

        final Table table = readTableOrView();
        if (readIf("ADD")) {
            final Prepared command = parseAlterTableAddConstraintIf(table.getName(), table.getSchema());
            if (command != null) { return command; }
            return parseAlterTableAddColumn(table);
        }
        else if (readIf("SET")) {
            read("REFERENTIAL_INTEGRITY");
            int type;
            if (readIf("TRUE")) {
                type = AlterTableSet.REFERENTIAL_INTEGRITY_TRUE;
            }
            else {
                read("FALSE");
                type = AlterTableSet.REFERENTIAL_INTEGRITY_FALSE;
            }
            final AlterTableSet command = new AlterTableSet(session, table.getSchema(), type);
            command.setTableName(table.getName());
            if (readIf("CHECK")) {
                command.setCheckExisting(true);
            }
            else if (readIf("NOCHECK")) {
                command.setCheckExisting(false);
            }
            return command;
        }
        else if (readIf("RENAME")) {
            read("TO");
            final String newName = readIdentifierWithSchema(table.getSchema().getName());
            checkSchema(table.getSchema());
            final AlterTableRename command = new AlterTableRename(session, getSchema(), internalQuery);
            command.setOldTable(table);
            command.setNewTableName(newName);
            return command;
        }
        else if (readIf("DROP")) {
            if (readIf("CONSTRAINT")) {
                boolean ifExists = readIfExists(false);
                final String constraintName = readIdentifierWithSchema(table.getSchema().getName());
                ifExists = readIfExists(ifExists);
                checkSchema(table.getSchema());
                final AlterTableDropConstraint command = new AlterTableDropConstraint(session, getSchema(), ifExists, internalQuery);
                command.setConstraintName(constraintName);
                return command;
            }
            else if (readIf("PRIMARY")) {
                read("KEY");
                final Index idx = table.getPrimaryKey();
                final DropIndex command = new DropIndex(session, table.getSchema());
                command.setIndexName(idx.getName());
                return command;
            }
            else {
                readIf("COLUMN");
                final AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema(), internalQuery);
                command.setType(AlterTableAlterColumn.DROP);
                final String columnName = readColumnIdentifier();
                command.setTable(table);
                command.setOldColumn(table.getColumn(columnName));
                return command;
            }
        }
        else if (readIf("ALTER")) {
            readIf("COLUMN");
            final String columnName = readColumnIdentifier();
            final Column column = table.getColumn(columnName);
            if (readIf("RENAME")) {
                read("TO");
                final AlterTableRenameColumn command = new AlterTableRenameColumn(session);
                command.setTable(table);
                command.setColumn(column);
                final String newName = readColumnIdentifier();
                command.setNewColumnName(newName);
                return command;
            }
            else if (readIf("SET")) {
                if (readIf("DATA")) {
                    // Derby compatibility
                    read("TYPE");
                    final Column newColumn = parseColumnForTable(columnName);
                    final AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema(), internalQuery);
                    command.setTable(table);
                    command.setType(AlterTableAlterColumn.CHANGE_TYPE);
                    command.setOldColumn(column);
                    command.setNewColumn(newColumn);
                    return command;
                }
                final AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema(), internalQuery);
                command.setTable(table);
                command.setOldColumn(column);
                if (readIf("NULL")) {
                    command.setType(AlterTableAlterColumn.NULL);
                    return command;
                }
                else if (readIf("NOT")) {
                    read("NULL");
                    command.setType(AlterTableAlterColumn.NOT_NULL);
                    return command;
                }
                else if (readIf("DEFAULT")) {
                    final Expression defaultExpression = readExpression();
                    command.setType(AlterTableAlterColumn.DEFAULT);
                    command.setDefaultExpression(defaultExpression);
                    return command;
                }
            }
            else if (readIf("RESTART")) {
                readIf("WITH");
                final Expression start = readExpression();
                final AlterSequence command = new AlterSequence(session, table.getSchema());
                command.setColumn(column);
                command.setStartWith(start);
                return command;
            }
            else if (readIf("SELECTIVITY")) {
                final AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema(), internalQuery);
                command.setTable(table);
                command.setType(AlterTableAlterColumn.SELECTIVITY);
                command.setOldColumn(column);
                command.setSelectivity(readExpression());
                return command;
            }
            else {
                final Column newColumn = parseColumnForTable(columnName);
                final AlterTableAlterColumn command = new AlterTableAlterColumn(session, table.getSchema(), internalQuery);
                command.setTable(table);
                command.setType(AlterTableAlterColumn.CHANGE_TYPE);
                command.setOldColumn(column);
                command.setNewColumn(newColumn);
                return command;
            }
        }
        throw getSyntaxError();
    }

    private AlterTableAlterColumn parseAlterTableAddColumn(final Table table) throws SQLException {

        readIf("COLUMN");
        final Schema schema = table.getSchema();
        final AlterTableAlterColumn command = new AlterTableAlterColumn(session, schema, internalQuery);
        command.setType(AlterTableAlterColumn.ADD);
        command.setTable(table);
        final String columnName = readColumnIdentifier();
        final Column column = parseColumnForTable(columnName);
        command.setNewColumn(column);
        if (readIf("BEFORE")) {
            command.setAddBefore(readColumnIdentifier());
        }
        return command;
    }

    private int parseAction() throws SQLException {

        if (readIf("CASCADE")) {
            return ConstraintReferential.CASCADE;
        }
        else if (readIf("RESTRICT")) {
            return ConstraintReferential.RESTRICT;
        }
        else if (readIf("NO")) {
            read("ACTION");
            return ConstraintReferential.RESTRICT;
        }
        else {
            read("SET");
            if (readIf("NULL")) { return ConstraintReferential.SET_NULL; }
            read("DEFAULT");
            return ConstraintReferential.SET_DEFAULT;
        }
    }

    private Prepared parseAlterTableAddConstraintIf(final String tableName, final Schema schema) throws SQLException {

        String constraintName = null, comment = null;
        boolean ifNotExists = false;
        if (readIf("CONSTRAINT")) {
            ifNotExists = readIfNoExists();
            constraintName = readIdentifierWithSchema(schema.getName());
            checkSchema(schema);
            comment = readCommentIf();
        }
        if (readIf("PRIMARY")) {
            read("KEY");
            final AlterTableAddConstraint command = new AlterTableAddConstraint(session, schema, ifNotExists, internalQuery);
            command.setType(AlterTableAddConstraint.PRIMARY_KEY);
            command.setComment(comment);
            command.setConstraintName(constraintName);
            command.setTableName(tableName);
            if (readIf("HASH")) {
                command.setPrimaryKeyHash(true);
            }
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                final String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
            return command;
        }
        else if (database.getMode().indexDefinitionInCreateTable && (readIf("INDEX") || readIf("KEY"))) {
            // MySQL
            final CreateIndex command = new CreateIndex(session, schema);
            command.setComment(comment);
            command.setTableName(tableName);
            if (!readIf("(")) {
                command.setIndexName(readUniqueIdentifier());
                read("(");
            }
            command.setIndexColumns(parseIndexColumnList());
            return command;
        }
        AlterTableAddConstraint command;
        if (readIf("CHECK")) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists, internalQuery);
            command.setType(AlterTableAddConstraint.CHECK);
            command.setCheckExpression(readExpression());
        }
        else if (readIf("UNIQUE")) {
            readIf("KEY");
            readIf("INDEX");
            command = new AlterTableAddConstraint(session, schema, ifNotExists, internalQuery);
            command.setType(AlterTableAddConstraint.UNIQUE);
            if (!readIf("(")) {
                constraintName = readUniqueIdentifier();
                read("(");
            }
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                final String indexName = readIdentifierWithSchema();
                command.setIndex(getSchema().findIndex(session, indexName));
            }
        }
        else if (readIf("FOREIGN")) {
            command = new AlterTableAddConstraint(session, schema, ifNotExists, internalQuery);
            command.setType(AlterTableAddConstraint.REFERENTIAL);
            read("KEY");
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            if (readIf("INDEX")) {
                final String indexName = readIdentifierWithSchema();
                command.setIndex(schema.findIndex(session, indexName));
            }
            read("REFERENCES");
            parseReferences(command, schema, tableName);
        }
        else {
            if (constraintName != null) { throw getSyntaxError(); }
            return null;
        }
        if (readIf("NOCHECK")) {
            command.setCheckExisting(false);
        }
        else {
            readIf("CHECK");
            command.setCheckExisting(true);
        }
        command.setTableName(tableName);
        command.setConstraintName(constraintName);
        command.setComment(comment);
        return command;
    }

    private void parseReferences(final AlterTableAddConstraint command, final Schema schema, final String tableName) throws SQLException {

        if (readIf("(")) {
            command.setRefTableName(schema, tableName);
            command.setRefIndexColumns(parseIndexColumnList());
        }
        else {
            final String refTableName = readIdentifierWithSchema(schema.getName());
            command.setRefTableName(getSchema(), refTableName);
            if (readIf("(")) {
                command.setRefIndexColumns(parseIndexColumnList());
            }
        }
        if (readIf("INDEX")) {
            final String indexName = readIdentifierWithSchema();
            command.setRefIndex(getSchema().findIndex(session, indexName));
        }
        while (readIf("ON")) {
            if (readIf("DELETE")) {
                command.setDeleteAction(parseAction());
            }
            else {
                read("UPDATE");
                command.setUpdateAction(parseAction());
            }
        }
        if (readIf("NOT")) {
            read("DEFERRABLE");
        }
        else {
            readIf("DEFERRABLE");
        }
    }

    private CreateLinkedTable parseCreateLinkedTable(final boolean temp, final boolean globalTemp, final boolean force) throws SQLException {

        read("TABLE");
        final boolean ifNotExists = readIfNoExists();
        final String tableName = readIdentifierWithSchema();
        final CreateLinkedTable command = new CreateLinkedTable(session, getSchema());
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setForce(force);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        read("(");
        command.setDriver(readString());
        read(",");
        command.setUrl(readString());
        read(",");
        command.setUser(readString());
        read(",");
        command.setPassword(readString());
        read(",");
        String originalTable = readString();
        if (readIf(",")) {
            command.setOriginalSchema(originalTable);
            originalTable = readString();
        }
        command.setOriginalTable(originalTable);
        read(")");
        if (readIf("EMIT")) {
            read("UPDATES");
            command.setEmitUpdates(true);
        }
        else if (readIf("READONLY")) {
            command.setReadOnly(true);
        }
        return command;
    }

    /*
     * START OF H2O ############################################################
     */

    /**
     * @return
     * @throws SQLException
     */
    private Prepared parseGet() throws SQLException {

        if (readIf("RMI")) {
            read("PORT");

            String databaseLocation = null;
            if (readIf("AT")) {
                databaseLocation = readExpression().toString();
            }

            final GetRmiPort command = new GetRmiPort(session, getSchema(), databaseLocation);

            return command;
        }
        else if (readIf("REPLICATION")) {
            read("FACTOR");

            final String tableName = readExpression().toString();

            final GetReplicationFactor command = new GetReplicationFactor(session, tableName);

            return command;
        }
        else if (readIf("METAREPLICATION")) {
            read("FACTOR");

            final String tableName = readExpression().toString();

            final GetMetaDatReplicationFactor command = new GetMetaDatReplicationFactor(session, tableName);

            return command;
        }
        return null;
    }

    /**
     * @return
     * @throws SQLException
     */
    private Prepared parseMigrate() throws SQLException {

        if (readIf("SYSTEMTABLE")) {

            boolean noReplicateToPreviousInstance = false;

            if (readIf("NO_REPLICATE")) {
                noReplicateToPreviousInstance = true;
            }

            return new MigrateSystemTable(session, null, noReplicateToPreviousInstance);
        }
        else if (readIf("TABLEMANAGER")) {
            final String tableName = readIdentifierWithSchema();
            final Schema schema = getSchema();
            return new MigrateTableManager(session, schema, tableName);
        }
        else {
            throw new SQLException("Could parse migrate command.");
        }
    }

    /**
     * @return
     * @throws SQLException
     */
    private Prepared parseRecreate() throws SQLException {

        if (readIf("TABLEMANAGER")) {

            final String tableName = readIdentifierWithSchema();
            final Schema schema = getSchema();

            String from = null;
            if (readIf("FROM")) {
                from = readString();
            }

            return new RecreateTableManager(session, schema, tableName, from);
        }
        else {
            throw new SQLException("Could not parse migrate command.");
        }
    }

    private CreateReplica parseCreateReplica(final boolean temp, boolean globalTemp, final boolean persistent, final boolean empty) throws SQLException, RPCException {

        final boolean ifNotExists = readIfNoExists();
        final boolean updateData = readUpdateData();

        CreateReplica command = null;

        if (readIf("SCHEMA")) {
            /*
             * To copy the entire schema this section loops through all tables. Where there are local tables which are TableData objects
             * (i.e. not linked) a new create replica command is created. For the first table found the 'command' variable is instantiated.
             * For each subsequent table a new Create table object is created and added to the 'next' of the original command.
             */

            schemaName = readExpression().toString();
            final Schema s = getSchema();

            final ISystemTableMigratable systemTableRemote = session.getDatabase().getSystemTable();
            try {
                final java.util.Set<String> tables = systemTableRemote.getAllTablesInSchema(s.getName());

                int numTables = 0;

                for (final String shortTableName : tables) {

                    if (numTables == 0) {
                        command = new CreateReplica(session, s, false, updateData);

                        command.setPersistent(persistent);
                        command.setTemporary(temp);
                        command.setGlobalTemporary(globalTemp);
                        command.setIfNotExists(ifNotExists);
                        command.setTableName(shortTableName);
                        command.setComment(readCommentIf());
                    }
                    else {
                        final CreateReplica next = new CreateReplica(session, getSchema(), false, updateData);
                        next.setTableName(shortTableName);

                        next.setPersistent(persistent);
                        next.setTemporary(temp);
                        next.setGlobalTemporary(globalTemp);
                        next.setIfNotExists(ifNotExists);
                        next.setComment(readCommentIf());

                        command.addNextCreateReplica(next);
                    }
                    numTables++;

                }
            }
            catch (final MovedException e) {
                throw new RPCException("System Table has moved.");
            }
        }
        else {

            String tableName = readIdentifierWithSchema();
            if (temp && globalTemp && "SESSION".equals(schemaName)) {
                // support weird syntax: declare global temporary table
                // session.xy
                // (...) not logged
                schemaName = session.getCurrentSchemaName();
                globalTemp = false;
            }
            checkForSchemaName();
            final Schema schema = getSchema();
            command = new CreateReplica(session, schema, empty, updateData);

            command.setPersistent(persistent);
            command.setTemporary(temp);
            command.setGlobalTemporary(globalTemp);
            command.setIfNotExists(ifNotExists);
            command.setTableName(tableName);
            command.setComment(readCommentIf());

            parseReplicaTypingInformation(empty, command, tableName, schema);

            while (readIf(",")) {
                tableName = readIdentifierWithSchema();
                checkForSchemaName();
                final CreateReplica next = new CreateReplica(session, getSchema(), empty, updateData);
                next.setTableName(tableName);
                command.addNextCreateReplica(next);

                next.setPersistent(persistent);
                next.setTemporary(temp);
                next.setGlobalTemporary(globalTemp);
                next.setIfNotExists(ifNotExists);
                next.setTableName(tableName);
                next.setComment(readCommentIf());

                parseReplicaTypingInformation(empty, command, tableName, schema);

            }

        }

        String whereReplicaWillBeCreated = null;
        String whereDataWillBeTakenFrom = null;

        if (readIf("ON")) {
            whereReplicaWillBeCreated = readString();
        }

        boolean x = true;

        if (readIf("FROM")) {
            whereDataWillBeTakenFrom = readString();

            if (whereDataWillBeTakenFrom != null) {
                x = false;
            }
        }

        if (command != null) {
            command.setOriginalLocation(whereDataWillBeTakenFrom, x);
            command.setReplicationLocation(whereReplicaWillBeCreated);
        }

        return command;
    }

    /**
     * Parses typing information for a CREATE REPLICA command. If the replica being created is for a table that is only just being created
     * then it is acceptable to pass typing information as part of the query. This method will only do anything if this is the case
     * (indicated by the parameter <em>empty</em> being true.
     * 
     * @param empty
     * @param command
     * @param tableName
     * @param schema
     * @throws SQLException
     */
    private void parseReplicaTypingInformation(final boolean empty, final CreateReplica command, final String tableName, final Schema schema) throws SQLException {

        if (empty) {
            if (readIf("AS")) {
                command.setQuery(parseSelect());
            }
            else {
                read("(");
                if (!readIf(")")) {
                    do {
                        final Prepared c = parseAlterTableAddConstraintIf(tableName, schema);
                        if (c != null) {
                            command.addConstraintCommand(c);
                        }
                        else {
                            final String columnName = readColumnIdentifier();
                            final Column column = parseColumnForTable(columnName);
                            if (column.getAutoIncrement() && column.getPrimaryKey()) {
                                column.setPrimaryKey(false);
                                final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                                cols[0].columnName = column.getName();
                                final AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false, internalQuery);
                                pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                                pk.setTableName(tableName);
                                pk.setIndexColumns(cols);
                                command.addConstraintCommand(pk);
                            }
                            command.addColumn(column);
                            String constraintName = null;
                            if (readIf("CONSTRAINT")) {
                                constraintName = readColumnIdentifier();
                            }
                            if (readIf("PRIMARY")) {
                                read("KEY");
                                final boolean hash = readIf("HASH");
                                final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                                cols[0].columnName = column.getName();
                                final AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false, internalQuery);
                                pk.setPrimaryKeyHash(hash);
                                pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                                pk.setTableName(tableName);
                                pk.setIndexColumns(cols);
                                command.addConstraintCommand(pk);
                                if (readIf("AUTO_INCREMENT")) {
                                    parseAutoIncrement(column);
                                }
                            }
                            else if (readIf("UNIQUE")) {
                                final AlterTableAddConstraint unique = new AlterTableAddConstraint(session, schema, false, internalQuery);
                                unique.setConstraintName(constraintName);
                                unique.setType(AlterTableAddConstraint.UNIQUE);
                                final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                                cols[0].columnName = columnName;
                                unique.setIndexColumns(cols);
                                unique.setTableName(tableName);
                                command.addConstraintCommand(unique);
                            }
                            if (readIf("CHECK")) {
                                final Expression expr = readExpression();
                                column.addCheckConstraint(session, expr);
                            }
                            if (readIf("REFERENCES")) {
                                final AlterTableAddConstraint ref = new AlterTableAddConstraint(session, schema, false, internalQuery);
                                ref.setConstraintName(constraintName);
                                ref.setType(AlterTableAddConstraint.REFERENTIAL);
                                final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                                cols[0].columnName = columnName;
                                ref.setIndexColumns(cols);
                                ref.setTableName(tableName);
                                parseReferences(ref, schema, tableName);
                                command.addConstraintCommand(ref);
                            }
                        }
                    }
                    while (readIfMore());
                }
                if (readIf("AS")) {
                    command.setQuery(parseSelect());
                }
            }
        }
    }

    /*
     * END OF H2O ############################################################
     */

    /**
     * Check whether the schema being called exists, and if it doesn't create it. This is called when a new replica is being created.
     * 
     * @throws SQLException
     */
    private void checkForSchemaName() throws SQLException {

        final Schema schema = database.findSchema(schemaName);

        if (schema != null) { return; // the schema already exists.
        }

        final CreateSchema command = new CreateSchema(session);
        command.setIfNotExists(true);
        command.setSchemaName(schemaName);
        command.setAuthorization(session.getUser().getName());
        try {
            command.update();
        }
        catch (final SQLException e) {
            throw new SQLException("Failed to create new schema for replica.");
        }

    }

    private CreateTable parseCreateTable(final boolean temp, boolean globalTemp, final boolean persistent) throws SQLException {

        final boolean ifNotExists = readIfNoExists();
        final String tableName = readIdentifierWithSchema();
        if (temp && globalTemp && "SESSION".equals(schemaName)) {
            // support weird syntax: declare global temporary table session.xy
            // (...) not logged
            schemaName = session.getCurrentSchemaName();
            globalTemp = false;
        }
        final Schema schema = getSchema();
        final CreateTable command = new CreateTable(session, schema);
        command.setPersistent(persistent);
        command.setTemporary(temp);
        command.setGlobalTemporary(globalTemp);
        command.setIfNotExists(ifNotExists);
        command.setTableName(tableName);
        command.setComment(readCommentIf());
        if (readIf("AS")) {
            command.setQuery(parseSelect());
        }
        else {
            read("(");
            if (!readIf(")")) {
                do {
                    final Prepared c = parseAlterTableAddConstraintIf(tableName, schema);
                    if (c != null) {
                        command.addConstraintCommand(c);
                    }
                    else {
                        final String columnName = readColumnIdentifier();
                        final Column column = parseColumnForTable(columnName);
                        if (column.getAutoIncrement() && column.getPrimaryKey()) {
                            column.setPrimaryKey(false);
                            final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = column.getName();
                            final AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false, internalQuery);
                            pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                            pk.setTableName(tableName);
                            pk.setIndexColumns(cols);
                            command.addConstraintCommand(pk);
                        }
                        command.addColumn(column);
                        String constraintName = null;
                        if (readIf("CONSTRAINT")) {
                            constraintName = readColumnIdentifier();
                        }
                        if (readIf("PRIMARY")) {
                            read("KEY");
                            final boolean hash = readIf("HASH");
                            final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = column.getName();
                            final AlterTableAddConstraint pk = new AlterTableAddConstraint(session, schema, false, internalQuery);
                            pk.setPrimaryKeyHash(hash);
                            pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
                            pk.setTableName(tableName);
                            pk.setIndexColumns(cols);
                            command.addConstraintCommand(pk);
                            if (readIf("AUTO_INCREMENT")) {
                                parseAutoIncrement(column);
                            }
                        }
                        else if (readIf("UNIQUE")) {
                            final AlterTableAddConstraint unique = new AlterTableAddConstraint(session, schema, false, internalQuery);
                            unique.setConstraintName(constraintName);
                            unique.setType(AlterTableAddConstraint.UNIQUE);
                            final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = columnName;
                            unique.setIndexColumns(cols);
                            unique.setTableName(tableName);
                            command.addConstraintCommand(unique);
                        }
                        if (readIf("CHECK")) {
                            final Expression expr = readExpression();
                            column.addCheckConstraint(session, expr);
                        }
                        if (readIf("REFERENCES")) {
                            final AlterTableAddConstraint ref = new AlterTableAddConstraint(session, schema, false, internalQuery);
                            ref.setConstraintName(constraintName);
                            ref.setType(AlterTableAddConstraint.REFERENTIAL);
                            final IndexColumn[] cols = new IndexColumn[]{new IndexColumn()};
                            cols[0].columnName = columnName;
                            ref.setIndexColumns(cols);
                            ref.setTableName(tableName);
                            parseReferences(ref, schema, tableName);
                            command.addConstraintCommand(ref);
                        }
                    }
                }
                while (readIfMore());
            }
            if (readIf("AS")) {
                command.setQuery(parseSelect());
            }
        }
        if (temp) {
            if (readIf("ON")) {
                read("COMMIT");
                if (readIf("DROP")) {
                    command.setOnCommitDrop();
                }
                else if (readIf("DELETE")) {
                    read("ROWS");
                    command.setOnCommitTruncate();
                }
            }
            else if (readIf("NOT")) {
                read("LOGGED");
            }
        }
        if (readIf("CLUSTERED")) {
            command.setClustered(true);
        }
        return command;
    }

    private int getCompareType(final int tokenType) {

        switch (tokenType) {
            case EQUAL:
                return Comparison.EQUAL;
            case BIGGER_EQUAL:
                return Comparison.BIGGER_EQUAL;
            case BIGGER:
                return Comparison.BIGGER;
            case SMALLER:
                return Comparison.SMALLER;
            case SMALLER_EQUAL:
                return Comparison.SMALLER_EQUAL;
            case NOT_EQUAL:
                return Comparison.NOT_EQUAL;
            default:
                return -1;
        }
    }

    /**
     * Add double quotes around an identifier if required.
     * 
     * @param s
     *            the identifier
     * @return the quoted identifier
     */
    public static String quoteIdentifier(final String s) {

        if (s == null || s.length() == 0) { return "\"\""; }
        char c = s.charAt(0);
        // lowercase a-z is quoted as well
        if (!Character.isLetter(c) && c != '_' || Character.isLowerCase(c)) { return StringUtils.quoteIdentifier(s); }
        for (int i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_' || Character.isLowerCase(c)) { return StringUtils.quoteIdentifier(s); }
        }
        if (Parser.isKeyword(s, true)) { return StringUtils.quoteIdentifier(s); }
        return s;
    }

    public void setRightsChecked(final boolean rightsChecked) {

        this.rightsChecked = rightsChecked;
    }

    /**
     * Parse a SQL code snippet that represents an expression.
     * 
     * @param sql
     *            the code snippet
     * @return the expression object
     * @throws SQLException
     *             if the code snippet could not be parsed
     */
    public Expression parseExpression(final String sql) throws SQLException {

        parameters = new ObjectArray();
        initialize(sql);
        read();
        return readExpression();
    }

}
