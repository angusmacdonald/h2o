/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.fulltext;

// ## Java 1.4 begin ##
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.h2.api.CloseListener;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.expression.ExpressionColumn;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileSystem;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * This class implements the full text search based on Apache Lucene. Most methods can be called using SQL statements as well.
 */
public class FullTextLucene extends FullText {

    // ## Java 1.4 begin ##
    private static final String TRIGGER_PREFIX = "FTL_";

    private static final String SCHEMA = "FTL";

    private static final boolean STORE_DOCUMENT_TEXT_IN_INDEX = Boolean.getBoolean("h2.storeDocumentTextInIndex");

    private static final HashMap INDEX_MODIFIERS = new HashMap();

    private static final String FIELD_DATA = "DATA";

    private static final String FIELD_COLUMN_PREFIX = "_";

    private static final String FIELD_QUERY = "QUERY";

    // ## Java 1.4 end ##

    /**
     * Initializes full text search functionality for this database. This adds the following Java functions to the database:
     * <ul>
     * <li>FTL_CREATE_INDEX(schemaNameString, tableNameString, columnListString)</li>
     * <li>FTL_SEARCH(queryString, limitInt, offsetInt): result set</li>
     * <li>FTL_REINDEX()</li>
     * <li>FTL_DROP_ALL()</li>
     * </ul>
     * It also adds a schema FTL to the database where bookkeeping information is stored. This function may be called from a Java
     * application, or by using the SQL statements:
     * 
     * <pre>
     * CREATE ALIAS IF NOT EXISTS FTL_INIT FOR
     *      &quot;org.h2.fulltext.FullTextLucene.init&quot;;
     * CALL FTL_INIT();
     * </pre>
     * 
     * @param conn
     *            the connection
     */
    // ## Java 1.4 begin ##
    public static void init(Connection conn) throws SQLException {

        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA + ".INDEXES(SCHEMA VARCHAR, TABLE VARCHAR, COLUMNS VARCHAR, PRIMARY KEY(SCHEMA, TABLE))");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_CREATE_INDEX FOR \"" + FullTextLucene.class.getName() + ".createIndex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH FOR \"" + FullTextLucene.class.getName() + ".search\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH_DATA FOR \"" + FullTextLucene.class.getName() + ".searchData\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_REINDEX FOR \"" + FullTextLucene.class.getName() + ".reindex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_ALL FOR \"" + FullTextLucene.class.getName() + ".dropAll\"");
        try {
            getIndexModifier(conn);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    // ## Java 1.4 end ##

    /**
     * Create a new full text index for a table and column list. Each table may only have one index at any time.
     * 
     * @param conn
     *            the connection
     * @param schema
     *            the schema name of the table (case sensitive)
     * @param table
     *            the table name (case sensitive)
     * @param columnList
     *            the column list (null for all columns)
     */
    // ## Java 1.4 begin ##
    public static void createIndex(Connection conn, String schema, String table, String columnList) throws SQLException {

        init(conn);
        PreparedStatement prep = conn.prepareStatement("INSERT INTO " + SCHEMA + ".INDEXES(SCHEMA, TABLE, COLUMNS) VALUES(?, ?, ?)");
        prep.setString(1, schema);
        prep.setString(2, table);
        prep.setString(3, columnList);
        prep.execute();
        createTrigger(conn, schema, table);
        indexExistingRows(conn, schema, table);
    }

    // ## Java 1.4 end ##

    /**
     * Re-creates the full text index for this database.
     * 
     * @param conn
     *            the connection
     */
    // ## Java 1.4 begin ##
    public static void reindex(Connection conn) throws SQLException {

        init(conn);
        removeAllTriggers(conn, TRIGGER_PREFIX);
        removeIndexFiles(conn);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".INDEXES");
        while (rs.next()) {
            String schema = rs.getString("SCHEMA");
            String table = rs.getString("TABLE");
            createTrigger(conn, schema, table);
            indexExistingRows(conn, schema, table);
        }
    }

    // ## Java 1.4 end ##

    /**
     * Drops all full text indexes from the database.
     * 
     * @param conn
     *            the connection
     */
    // ## Java 1.4 begin ##
    public static void dropAll(Connection conn) throws SQLException {

        Statement stat = conn.createStatement();
        stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA);
        removeAllTriggers(conn, TRIGGER_PREFIX);
        removeIndexFiles(conn);
    }

    // ## Java 1.4 end ##

    /**
     * Searches from the full text index for this database. The returned result set has the following column:
     * <ul>
     * <li>QUERY (varchar): The query to use to get the data. The query does not include 'SELECT * FROM '. Example: PUBLIC.TEST WHERE ID = 1
     * </li>
     * </ul>
     * 
     * @param conn
     *            the connection
     * @param text
     *            the search query
     * @param limit
     *            the maximum number of rows or 0 for no limit
     * @param offset
     *            the offset or 0 for no offset
     * @return the result set
     */
    // ## Java 1.4 begin ##
    public static ResultSet search(Connection conn, String text, int limit, int offset) throws SQLException {

        return search(conn, text, limit, offset, false);
    }

    // ## Java 1.4 end ##

    /**
     * Searches from the full text index for this database. The result contains the primary key data as an array. The returned result set
     * has the following columns:
     * <ul>
     * <li>SCHEMA (varchar): The schema name. Example: PUBLIC</li>
     * <li>TABLE (varchar): The table name. Example: TEST</li>
     * <li>COLUMNS (array of varchar): Comma separated list of quoted column names. The column names are quoted if necessary. Example: (ID)</li>
     * <li>KEYS (array of values): Comma separated list of values. Example: (1)</li>
     * </ul>
     * 
     * @param conn
     *            the connection
     * @param text
     *            the search query
     * @param limit
     *            the maximum number of rows or 0 for no limit
     * @param offset
     *            the offset or 0 for no offset
     * @return the result set
     */
    // ## Java 1.4 begin ##
    public static ResultSet searchData(Connection conn, String text, int limit, int offset) throws SQLException {

        return search(conn, text, limit, offset, true);
    }

    private static SQLException convertException(Exception e) {

        SQLException e2 = new SQLException("FULLTEXT", "Error while indexing document");
        e2.initCause(e);
        return e2;
    }

    private static void createTrigger(Connection conn, String schema, String table) throws SQLException {

        Statement stat = conn.createStatement();
        String trigger = StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
        stat.execute("DROP TRIGGER IF EXISTS " + trigger);
        StringBuilder buff = new StringBuilder("CREATE TRIGGER IF NOT EXISTS ");
        buff.append(trigger);
        buff.append(" AFTER INSERT, UPDATE, DELETE ON ");
        buff.append(StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(table));
        buff.append(" FOR EACH ROW CALL \"");
        buff.append(FullTextLucene.FullTextTrigger.class.getName());
        buff.append("\"");
        stat.execute(buff.toString());
    }

    private static IndexModifier getIndexModifier(Connection conn) throws SQLException {

        String path = getIndexPath(conn);
        IndexModifier indexer;
        synchronized (INDEX_MODIFIERS) {
            indexer = (IndexModifier) INDEX_MODIFIERS.get(path);
            if (indexer == null) {
                try {
                    boolean recreate = !IndexReader.indexExists(path);
                    Analyzer analyzer = new StandardAnalyzer();
                    indexer = new IndexModifier(path, analyzer, recreate);
                }
                catch (IOException e) {
                    throw convertException(e);
                }
                INDEX_MODIFIERS.put(path, indexer);
            }
        }
        return indexer;
    }

    private static String getIndexPath(Connection conn) throws SQLException {

        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("CALL DATABASE_PATH()");
        rs.next();
        String path = rs.getString(1);
        if (path == null) { throw new SQLException("FULLTEXT", "Fulltext search for in-memory databases is not supported."); }
        rs.close();
        return path;
    }

    private static void indexExistingRows(Connection conn, String schema, String table) throws SQLException {

        FullTextLucene.FullTextTrigger existing = new FullTextLucene.FullTextTrigger();
        existing.init(conn, schema, null, table, false, Trigger.INSERT);
        StringBuilder buff = new StringBuilder("SELECT * FROM ");
        buff.append(StringUtils.quoteIdentifier(schema) + "." + StringUtils.quoteIdentifier(table));
        ResultSet rs = conn.createStatement().executeQuery(buff.toString());
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            existing.fire(conn, null, row);
        }
    }

    private static void removeIndexFiles(Connection conn) throws SQLException {

        String path = getIndexPath(conn);
        IndexModifier index = (IndexModifier) INDEX_MODIFIERS.get(path);
        if (index != null) {
            INDEX_MODIFIERS.remove(path);
            try {
                index.flush();
                index.close();
            }
            catch (IOException e) {
                throw convertException(e);
            }
        }
        FileSystem.getInstance(path).deleteRecursive(path);
    }

    private static ResultSet search(Connection conn, String text, int limit, int offset, boolean data) throws SQLException {

        SimpleResultSet result = createResultSet(data);
        if (conn.getMetaData().getURL().startsWith("jdbc:columnlist:")) {
            // this is just to query the result set columns
            return result;
        }
        String path = getIndexPath(conn);
        try {
            IndexModifier indexer = getIndexModifier(conn);
            indexer.flush();
            IndexReader reader = IndexReader.open(path);
            Analyzer analyzer = new StandardAnalyzer();
            Searcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser(FIELD_DATA, analyzer);
            Query query = parser.parse(text);
            Hits hits = searcher.search(query);
            int max = hits.length();
            if (limit == 0) {
                limit = max;
            }
            for (int i = 0; i < limit && i + offset < max; i++) {
                Document doc = hits.doc(i + offset);
                String q = doc.get(FIELD_QUERY);
                if (data) {
                    int idx = q.indexOf(" WHERE ");
                    JdbcConnection c = (JdbcConnection) conn;
                    Session session = (Session) c.getSession();
                    Parser p = new Parser(session, true);
                    String tab = q.substring(0, idx);
                    ExpressionColumn expr = (ExpressionColumn) p.parseExpression(tab);
                    String schemaName = expr.getOriginalTableAliasName();
                    String tableName = expr.getColumnName();
                    q = q.substring(idx + " WHERE ".length());
                    Object[][] columnData = parseKey(conn, q);
                    Object[] row = new Object[]{schemaName, tableName, columnData[0], columnData[1]};
                    result.addRow(row);
                }
                else {
                    result.addRow(new Object[]{q});
                }
            }
            // TODO keep it open if possible
            reader.close();
        }
        catch (Exception e) {
            throw convertException(e);
        }
        return result;
    }

    // ## Java 1.4 end ##

    /**
     * Trigger updates the index when a inserting, updating, or deleting a row.
     */
    public static class FullTextTrigger
    // ## Java 1.4 begin ##
                    implements Trigger, CloseListener
    // ## Java 1.4 end ##
    {

        // ## Java 1.4 begin ##
        private String schema;

        private String table;

        private int[] keys;

        private int[] indexColumns;

        private String[] columns;

        private int[] columnTypes;

        private String indexPath;

        private IndexModifier indexModifier;

        // ## Java 1.4 end ##

        /**
         * INTERNAL
         */
        // ## Java 1.4 begin ##
        public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {

            this.schema = schemaName;
            this.table = tableName;
            this.indexPath = getIndexPath(conn);
            this.indexModifier = getIndexModifier(conn);
            ArrayList keyList = new ArrayList();
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getColumns(null, JdbcUtils.escapeMetaDataPattern(schemaName), JdbcUtils.escapeMetaDataPattern(tableName), null);
            ArrayList columnList = new ArrayList();
            while (rs.next()) {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
            columnTypes = new int[columnList.size()];
            columns = new String[columnList.size()];
            columnList.toArray(columns);
            rs = meta.getColumns(null, JdbcUtils.escapeMetaDataPattern(schemaName), JdbcUtils.escapeMetaDataPattern(tableName), null);
            for (int i = 0; rs.next(); i++) {
                columnTypes[i] = rs.getInt("DATA_TYPE");
            }
            if (keyList.size() == 0) {
                rs = meta.getPrimaryKeys(null, JdbcUtils.escapeMetaDataPattern(schemaName), tableName);
                while (rs.next()) {
                    keyList.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (keyList.size() == 0) { throw new SQLException("No primary key for table " + tableName); }
            ArrayList indexList = new ArrayList<String>();
            PreparedStatement prep = conn.prepareStatement("SELECT COLUMNS FROM " + SCHEMA + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
            prep.setString(1, schemaName);
            prep.setString(2, tableName);
            rs = prep.executeQuery();
            if (rs.next()) {
                String columns = rs.getString(1);
                if (columns != null) {
                    String[] list = StringUtils.arraySplit(columns, ',', true);
                    for (String element : list) {
                        indexList.add(element);
                    }
                }
            }
            if (indexList.size() == 0) {
                indexList.addAll(columnList);
            }
            keys = new int[keyList.size()];
            setColumns(keys, keyList, columnList);
            indexColumns = new int[indexList.size()];
            setColumns(indexColumns, indexList, columnList);
        }

        // ## Java 1.4 end ##

        /**
         * INTERNAL
         */
        // ## Java 1.4 begin ##
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

            if (oldRow != null) {
                delete(oldRow);
            }
            if (newRow != null) {
                insert(newRow);
            }
        }

        // ## Java 1.4 end ##

        /**
         * INTERNAL
         */
        // ## Java 1.4 begin ##
        public void close() throws SQLException {

            if (indexModifier != null) {
                try {
                    indexModifier.flush();
                    indexModifier.close();
                    INDEX_MODIFIERS.remove(indexPath);
                    indexModifier = null;
                }
                catch (Exception e) {
                    throw convertException(e);
                }
            }
        }

        // ## Java 1.4 end ##

        /**
         * INTERNAL
         */
        public void remove() {

            // ignore
        }

        private void insert(Object[] row) throws SQLException {

            String query = getQuery(row);
            Document doc = new Document();
            doc.add(new Field(FIELD_QUERY, query, Field.Store.YES, Field.Index.UN_TOKENIZED));
            long time = System.currentTimeMillis();
            doc.add(new Field("modified", DateTools.timeToString(time, DateTools.Resolution.SECOND), Field.Store.YES, Field.Index.UN_TOKENIZED));
            StringBuilder allData = new StringBuilder();
            for (int i = 0; i < indexColumns.length; i++) {
                int index = indexColumns[i];
                String columnName = columns[index];
                String data = asString(row[index], columnTypes[index]);
                doc.add(new Field(FIELD_COLUMN_PREFIX + columnName, data, Field.Store.NO, Field.Index.TOKENIZED));
                if (i > 0) {
                    allData.append(" ");
                }
                allData.append(data);
            }
            Field.Store storeText = STORE_DOCUMENT_TEXT_IN_INDEX ? Field.Store.YES : Field.Store.NO;
            doc.add(new Field(FIELD_DATA, allData.toString(), storeText, Field.Index.TOKENIZED));
            try {
                indexModifier.addDocument(doc);
            }
            catch (IOException e) {
                throw convertException(e);
            }
        }

        private void delete(Object[] row) throws SQLException {

            String query = getQuery(row);
            try {
                Term term = new Term(FIELD_QUERY, query);
                indexModifier.deleteDocuments(term);
            }
            catch (IOException e) {
                throw convertException(e);
            }
        }

        private String getQuery(Object[] row) throws SQLException {

            StringBuilder buff = new StringBuilder();
            if (schema != null) {
                buff.append(StringUtils.quoteIdentifier(schema));
                buff.append(".");
            }
            buff.append(StringUtils.quoteIdentifier(table));
            buff.append(" WHERE ");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0) {
                    buff.append(" AND ");
                }
                int columnIndex = keys[i];
                buff.append(StringUtils.quoteIdentifier(columns[columnIndex]));
                Object o = row[columnIndex];
                if (o == null) {
                    buff.append(" IS NULL");
                }
                else {
                    buff.append("=");
                    buff.append(FullText.quoteSQL(o, columnTypes[columnIndex]));
                }
            }
            String key = buff.toString();
            return key;
        }

    }

}
