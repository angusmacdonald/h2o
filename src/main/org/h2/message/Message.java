/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import org.h2.constant.ErrorCode;
import org.h2.jdbc.JdbcSQLException;
import org.h2.util.Resources;
import org.h2.util.StringUtils;

/**
 * Messages used in the database engine. Use the PropertiesToUTF8 tool to translate properties files to UTF-8 and back. If the word 'SQL'
 * appears then the whole SQL statement must be a parameter, otherwise this may be added: '; SQL statement: ' + sql
 */
public class Message {

    private static final Properties MESSAGES = new Properties();

    static {
        try {
            final byte[] messages = Resources.get("/org/h2/res/_messages_en.properties");
            if (messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
            final String language = Locale.getDefault().getLanguage();
            if (!"en".equals(language)) {
                final byte[] translations = Resources.get("/org/h2/res/_messages_" + language + ".properties");
                // message: translated message + english
                // (otherwise certain applications don't work)
                if (translations != null) {
                    final Properties p = new Properties();
                    p.load(new ByteArrayInputStream(translations));
                    for (final Object element : p.entrySet()) {
                        final Entry e = (Entry) element;
                        final String key = (String) e.getKey();
                        final String translation = (String) e.getValue();
                        if (translation != null && !translation.startsWith("#")) {
                            final String original = MESSAGES.getProperty(key);
                            final String message = translation + "\n" + original;
                            MESSAGES.put(key, message);
                        }
                    }
                }
            }
        }
        catch (final IOException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    private Message() {

        // utility class
    }

    /**
     * Gets the SQL exception object for a specific error code.
     * 
     * @param errorCode
     *            the error code
     * @param p1
     *            the first parameter of the message
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(final int errorCode, final String p1) {

        return getSQLException(errorCode, new String[]{p1});
    }

    private static String translate(final String key, final String[] param) {

        String message = null;
        if (MESSAGES != null) {
            // Tomcat sets final static fields to null sometimes
            message = MESSAGES.getProperty(key);
        }
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (param != null) {
            final Object[] o = param;
            message = MessageFormat.format(message, o);
        }
        return message;
    }

    /**
     * Gets the SQL exception object for a specific error code.
     * 
     * @param errorCode
     *            the error code
     * @param params
     *            the list of parameters of the message
     * @param cause
     *            the cause of the exception
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(final int errorCode, final String[] params, final Throwable cause) {

        final String sqlstate = ErrorCode.getState(errorCode);
        final String message = translate(sqlstate, params);
        return new JdbcSQLException(message, null, sqlstate, errorCode, cause, null);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     * 
     * @param errorCode
     *            the error code
     * @param params
     *            the list of parameters of the message
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(final int errorCode, final String[] params) {

        return getSQLException(errorCode, params, null);
    }

    /**
     * Constructs a syntax error SQL exception.
     * 
     * @param sql
     *            the SQL statement
     * @param index
     *            the position of the error in the SQL statement
     * @return the SQLException object
     */
    public static SQLException getSyntaxError(String sql, final int index) {

        sql = StringUtils.addAsterisk(sql, index);
        return getSQLException(ErrorCode.SYNTAX_ERROR_1, sql);
    }

    /**
     * Constructs a syntax error SQL exception.
     * 
     * @param sql
     *            the SQL statement
     * @param index
     *            the position of the error in the SQL statement
     * @param expected
     *            the expected keyword at the given position
     * @return the SQLException object
     */
    public static SQLException getSyntaxError(String sql, final int index, final String expected) {

        sql = StringUtils.addAsterisk(sql, index);
        return getSQLException(ErrorCode.SYNTAX_ERROR_2, new String[]{sql, expected});
    }

    /**
     * Gets the SQL exception object for a specific error code.
     * 
     * @param errorCode
     *            the error code
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(final int errorCode) {

        return getSQLException(errorCode, (String) null);
    }

    /**
     * Gets a SQL exception meaning this feature is not supported.
     * 
     * @return the SQLException object
     */
    public static JdbcSQLException getUnsupportedException() {

        return getSQLException(ErrorCode.FEATURE_NOT_SUPPORTED);
    }

    /**
     * Gets a SQL exception meaning this value is invalid.
     * 
     * @param value
     *            the value passed
     * @param param
     *            the name of the parameter
     * @return the SQLException object
     */
    public static JdbcSQLException getInvalidValueException(final String value, final String param) {

        return getSQLException(ErrorCode.INVALID_VALUE_2, new String[]{value, param});
    }

    /**
     * Throw an internal error. This method seems to return an exception object, so that it can be used instead of 'return', but in fact it
     * always throws the exception.
     * 
     * @param s
     *            the message
     * @return the RuntimeException object
     * @throws RuntimeException
     *             the exception
     */
    public static RuntimeException throwInternalError(final String s) {

        final RuntimeException e = new RuntimeException(s);
        TraceSystem.traceThrowable(e);
        if (true) { throw e; }
        return e;
    }

    /**
     * Throw an internal error. This method seems to return an exception object, so that it can be used instead of 'return', but in fact it
     * always throws the exception.
     * 
     * @return the RuntimeException object
     */
    public static RuntimeException throwInternalError() {

        return throwInternalError("Unexpected code path");
    }

    /**
     * Gets an internal error.
     * 
     * @param s
     *            the message
     * @param e
     *            the root cause
     * @return the error object
     */
    public static Error getInternalError(final String s, final Exception e) {

        final Error e2 = new Error(s);
        // ## Java 1.4 begin ##
        e2.initCause(e);
        // ## Java 1.4 end ##
        TraceSystem.traceThrowable(e2);
        return e2;
    }

    /**
     * Attach a SQL statement to the exception if this is not already done.
     * 
     * @param e
     *            the original SQL exception
     * @param sql
     *            the SQL statement
     * @return the error object
     */
    public static SQLException addSQL(final SQLException e, final String sql) {

        if (e instanceof JdbcSQLException) {
            final JdbcSQLException j = (JdbcSQLException) e;
            if (j.getSQL() == null) {
                j.setSQL(sql);
            }
            return j;
        }
        return new JdbcSQLException(e.getMessage(), sql, e.getSQLState(), e.getErrorCode(), e, null);
    }

    /**
     * Convert an exception to a SQL exception using the default mapping.
     * 
     * @param e
     *            the root cause
     * @param sql
     *            the SQL statement or null if it is not known
     * @return the SQL exception object
     */
    public static SQLException convert(final Throwable e, final String sql) {

        final SQLException e2 = convert(e);
        if (e2 instanceof JdbcSQLException) {
            ((JdbcSQLException) e2).setSQL(sql);
        }
        return e2;
    }

    /**
     * Convert an exception to a SQL exception using the default mapping.
     * 
     * @param e
     *            the root cause
     * @return the SQL exception object
     */
    public static SQLException convert(Throwable e) {

        if (e instanceof InternalException) {
            e = ((InternalException) e).getOriginalCause();
        }
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        else if (e instanceof OutOfMemoryError) {
            return getSQLException(ErrorCode.OUT_OF_MEMORY, null, e);
        }
        else if (e instanceof InvocationTargetException) {
            final InvocationTargetException te = (InvocationTargetException) e;
            final Throwable t = te.getTargetException();
            if (t instanceof SQLException) { return (SQLException) t; }
            return getSQLException(ErrorCode.EXCEPTION_IN_FUNCTION, null, e);
        }
        else if (e instanceof IOException) { return getSQLException(ErrorCode.IO_EXCEPTION_1, new String[]{e.toString()}, e); }
        return getSQLException(ErrorCode.GENERAL_ERROR_1, new String[]{e.toString()}, e);
    }

    /**
     * Convert an IO exception to a SQL exception.
     * 
     * @param e
     *            the root cause
     * @param message
     *            the message
     * @return the SQL exception object
     */
    public static SQLException convertIOException(final IOException e, final String message) {

        if (message == null) { return getSQLException(ErrorCode.IO_EXCEPTION_1, new String[]{e.toString()}, e); }
        return getSQLException(ErrorCode.IO_EXCEPTION_2, new String[]{e.toString(), message}, e);
    }

    /**
     * Convert an exception to an internal runtime exception.
     * 
     * @param e
     *            the root cause
     * @return the error object
     */
    public static InternalException convertToInternal(final Exception e) {

        return new InternalException(e);
    }

    /**
     * Convert an exception to an IO exception.
     * 
     * @param e
     *            the root cause
     * @return the IO exception
     */
    public static IOException convertToIOException(Throwable e) {

        if (e instanceof JdbcSQLException) {
            final JdbcSQLException e2 = (JdbcSQLException) e;
            if (e2.getOriginalCause() != null) {
                e = e2.getOriginalCause();
            }
        }
        final IOException io = new IOException(e.toString());
        // ## Java 1.4 begin ##
        io.initCause(e);
        // ## Java 1.4 end ##
        return io;
    }

}
