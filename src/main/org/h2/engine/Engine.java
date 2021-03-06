/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2.engine;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.store.FileLock;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * The engine contains a map of all open databases. It is also responsible for opening and creating new databases. This is a singleton
 * class.
 */
public class Engine {

    private static final Engine INSTANCE = new Engine();

    private static final Map<String, Database> DATABASES = new HashMap<String, Database>();

    private static volatile long wrongPasswordDelay = SysProperties.DELAY_WRONG_PASSWORD_MIN;

    private Engine() {

        // Don't allow others to instantiate.
    }

    public static Engine getInstance() {

        return INSTANCE;
    }

    /**
     * H2O. Access a given database instance. It's possible there could be some concurrency issues here.
     * 
     * @param databaseName
     * @return
     */
    public static Database getDatabase(final String databaseName) {

        return DATABASES.get(databaseName);
    }

    private Session openSession(final ConnectionInfo ci, final boolean ifExists, final String cipher) throws SQLException {

        final String name = ci.getName();

        Database database;
        final boolean openNew = ci.getProperty("OPEN_NEW", false);
        if (openNew || ci.isUnnamedInMemory()) {
            database = null;
        }
        else {
            database = DATABASES.get(ci.getName());
        }
        User user = null;
        boolean opened = false;
        if (database == null) {
            if (ifExists && !Database.exists(name)) { throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, name); }

            database = new Database(name, ci, cipher);

            opened = true;
            if (database.getAllUsers().size() == 0) {
                // users is the last thing we add, so if no user is around,
                // the database is not initialized correctly
                user = new User(database, database.allocateObjectId(false, true), ci.getUserName(), false);
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getUserPasswordHash());
                database.setMasterUser(user);
            }
            if (!ci.isUnnamedInMemory()) {
                DATABASES.put(name, database);
            }
            database.opened();
        }
        synchronized (database) {
            if (database.isClosing()) { return null; }
            if (user == null) {
                if (database.validateFilePasswordHash(cipher, ci.getFilePasswordHash())) {
                    user = database.findUser(ci.getUserName());
                    if (user != null) {
                        if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                            user = null;
                        }
                    }
                }
                if (opened && (user == null || !user.getAdmin())) {
                    // reset - because the user is not an admin, and has no
                    // right to listen to exceptions
                    database.setEventListener(null);
                }
            }
            if (user == null) {
                database.removeSession(null);
                throw Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
            checkClustering(ci, database);
            final Session session = database.createSession(user);
            return session;
        }
    }

    /**
     * Open a database connection with the given connection information.
     * 
     * @param ci
     *            the connection information
     * @return the session
     */
    public Session getSession(final ConnectionInfo ci) throws SQLException {

        try {
            ConnectionInfo backup = null;
            final String lockMethodName = ci.getProperty("FILE_LOCK", null);
            final int fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
            if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
                try {
                    backup = (ConnectionInfo) ci.clone();
                }
                catch (final CloneNotSupportedException e) {
                }
            }
            final Session session = openSession(ci);
            validateUserAndPassword(true);
            if (backup != null) {
                session.setConnectionInfo(backup);
            }
            return session;
        }
        catch (final SQLException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    private synchronized Session openSession(final ConnectionInfo ci) throws SQLException {

        final boolean ifExists = ci.removeProperty("IFEXISTS", false);
        final boolean ignoreUnknownSetting = ci.removeProperty("IGNORE_UNKNOWN_SETTINGS", false);
        final String cipher = ci.removeProperty("CIPHER", null);
        Session session;
        while (true) {
            session = openSession(ci, ifExists, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            try {
                Thread.sleep(1);
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }
        final String[] keys = ci.getKeys();
        session.setAllowLiterals(true);
        for (final String setting : keys) {
            final String value = ci.getProperty(setting);
            try {
                final CommandInterface command = session.prepareCommand("SET " + Parser.quoteIdentifier(setting) + " " + value, Integer.MAX_VALUE);
                command.executeUpdate();
            }
            catch (final SQLException e) {
                if (!ignoreUnknownSetting) {
                    session.close();
                    throw e;
                }
            }
        }
        session.setAllowLiterals(false);
        session.commit(true);
        session.getDatabase().getTrace(Trace.SESSION).info("connected #" + session.getSessionId());
        return session;
    }

    private void checkClustering(final ConnectionInfo ci, final Database database) throws SQLException {

        final String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        final String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!StringUtils.equals(clusterSession, clusterDb)) {
                if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) { throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE); }
                throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, clusterDb);
            }
        }
    }

    /**
     * Called after a database has been closed, to remove the object from the list of open databases.
     * 
     * @param name
     *            the database name
     */
    public void close(final String name) {

        DATABASES.remove(name);
    }

    /**
     * This method is called after validating user name and password. If user name and password were correct, the sleep time is reset,
     * otherwise this method waits some time (to make brute force / rainbow table attacks harder) and then throws a 'wrong user or password'
     * exception. The delay is a bit randomized to protect against timing attacks. Also the delay doubles after each unsuccessful logins, to
     * make brute force attacks harder.
     * 
     * There is only one exception both for wrong user and for wrong password, to make it harder to get the list of user names. This method
     * must only be called from one place, so it is not possible from the stack trace to see if the user name was wrong or the password.
     * 
     * @param correct
     *            if the user name or the password was correct
     * @throws SQLException
     *             the exception 'wrong user or password'
     */
    private static void validateUserAndPassword(final boolean correct) throws SQLException {

        final int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = wrongPasswordDelay;
            if (delay > min && delay > 0) {
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (INSTANCE) {
                    // delay up to the last delay
                    // an attacker can't know how long it will be
                    delay = RandomUtils.nextSecureInt((int) delay);
                    try {
                        Thread.sleep(delay);
                    }
                    catch (final InterruptedException e) {
                        // ignore
                    }
                    wrongPasswordDelay = min;
                }
            }
        }
        else {
            // this method is not synchronized on the Engine, so that
            // regular successful attempts are not blocked
            synchronized (INSTANCE) {
                long delay = wrongPasswordDelay;
                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                }
                wrongPasswordDelay += wrongPasswordDelay;
                if (wrongPasswordDelay > max || wrongPasswordDelay < 0) {
                    wrongPasswordDelay = max;
                }
                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(RandomUtils.getSecureLong() % 100);
                    try {
                        Thread.sleep(delay);
                    }
                    catch (final InterruptedException e) {
                        // ignore
                    }
                }
                throw Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }

    /**
     * Used for JUnit Testing in H2O to preserve independance of tests.
     * 
     * @return
     */
    public Set<Database> getAllDatabases() {

        final Set<Database> result = new HashSet<Database>();
        result.addAll(DATABASES.values());
        return result;
    }
}
