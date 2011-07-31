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

package org.h2o.db;

import java.net.InetSocketAddress;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.recovery.SystemTableAccessException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * Proxy class exposed via RMI, allowing semi-parsed queries to be sent to remote replicas for execution.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstance implements IDatabaseInstanceRemote {

    /**
     * The parsed JDBC connection string for this database.
     */
    private final DatabaseID databaseURL;

    /**
     * Used to parse queries on this machine.
     */
    private final Parser parser;

    /**
     * Whether the database instance is alive or in the process of being shut down.
     */
    private boolean alive = true;

    private final Database database;

    public DatabaseInstance(final DatabaseID databaseURL, final Session session) {

        this.databaseURL = databaseURL;
        database = session.getDatabase();

        parser = new Parser(session, true);
    }

    @Override
    public int execute(final String query, final String transactionName, final boolean commitOperation) throws SQLException, RPCException {

        if (!database.isRunning() || database.isStarting()) { throw new SQLException("Database has not yet started."); }

        if (query == null) {
            ErrorHandling.hardError("Shouldn't happen.");
        }

        final Command command = parser.prepareCommand(query);

        try {
            if (commitOperation) { return command.update(); // This is a COMMIT.
            }
            /*
             * If called from here executeUpdate should always be told the query is part of a larger transaction, because it was
             * remotely initiated and consequently needs to wait for the remote machine to commit.
             */
            int result = command.executeUpdate(true);

            final int prepareResult = prepare(transactionName); // This wasn't a COMMIT. Execute a PREPARE.

            if (prepareResult != 0) {
                result = -1; //signifies an error.
            }
            return result;
        }
        finally {
            command.close();
        }
    }

    @Override
    public int prepare(final String transactionName) throws SQLException {

        final Command command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
        return command.executeUpdate();
    }

    @Override
    public String getConnectionString() {

        return databaseURL.getOriginalURL();
    }

    @Override
    public DatabaseID getURL() {

        return databaseURL;
    }

    @Override
    public DatabaseID getSystemTableURL() {

        final DatabaseID systemTableURL = database.getSystemTableReference().getSystemTableURL();
        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Responding to request for System Table location at database '" + database.getDatabaseLocation() + "'. " + "System table location: " + systemTableURL);

        return systemTableURL;
    }

    @Override
    public int executeUpdate(final String sql, final boolean systemTableCommand) throws RPCException, SQLException {

        try {

            if (!database.isRunning()) { throw new SQLException("Could not execute query. The database either hasn't fully started, or is being shut down. Query: " + sql); }

            Command command = null;
            if (systemTableCommand) {

                final Parser schemaParser = new Parser(database.getH2OSession(), true);
                command = schemaParser.prepareCommand(sql);
            }
            else {
                command = parser.prepareCommand(sql);
            }

            final int result = command.executeUpdate(false);
            command.close();

            return result;

        }
        catch (final SQLException e) {
            ErrorHandling.exceptionError(e, "Exception thrown while executing update on " + database.getID());
            throw e;
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Exception thrown while executing update on " + database.getID());
            return -1;
        }
    }

    @Override
    public ISystemTableMigratable recreateSystemTable() throws SystemTableAccessException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Responding to request to recreate System Table on '" + database.getDatabaseLocation() + "'.");

        final ISystemTableReference systemTableReference = database.getSystemTableReference();
        return systemTableReference.migrateSystemTableToLocalInstance(true, true);
    }

    @Override
    public boolean recreateTableManager(final TableInfo tableInfo, final DatabaseID previousLocation) throws RPCException {

        boolean success = false;
        try {
            executeUpdate("RECREATE TABLEMANAGER " + tableInfo.getFullTableName() + " FROM '" + previousLocation.sanitizedLocation() + "';", true);
            success = true;
        }
        catch (final SQLException e) {
            Diagnostic.trace(DiagnosticLevel.FULL, "Error re-creating table manager");
        }
        return success;
    }

    @Override
    public void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseID databaseURL) throws RPCException {

        database.getSystemTableReference().setSystemTableLocation(systemTableLocation, databaseURL);
    }

    @Override
    public ITableManagerRemote findTableManagerReference(final TableInfo ti, final boolean searchOnlyCache) throws RPCException {

        try {
            return database.getSystemTableReference().lookup(ti, true, searchOnlyCache);
        }
        catch (final SQLException e) {

            Diagnostic.trace(DiagnosticLevel.FULL, "Couldn't find Table Manager at this machine. Table Manager needs to be re-instantiated.");
            // TODO allow for re-instantiation at this point.

            throw new RPCException(e.getMessage());
        }
    }

    @Override
    public void setAlive(final boolean alive) {

        this.alive = alive;
    }

    @Override
    public boolean isAlive() {

        return alive;
    }

    @Override
    public int hashCode() {

        final int result = 31 + (databaseURL.getURL() == null ? 0 : databaseURL.getURL().hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object other) {

        try {
            final DatabaseInstance other_wrapper = (DatabaseInstance) other;

            return other_wrapper != null && (databaseURL.getURL() == other_wrapper.databaseURL.getURL() || databaseURL.getURL() != null && databaseURL.getURL().equals(other_wrapper.databaseURL.getURL()));
        }
        catch (final ClassCastException e) {
            return false;
        }
    }

    @Override
    public boolean isSystemTable() throws RPCException {

        return database.getSystemTableReference().isSystemTableLocal();
    }

    @Override
    public ISystemTableMigratable getSystemTable() throws RPCException {

        return database.getSystemTableReference().getLocalSystemTable();
    }

    @Override
    public InetSocketAddress getAddress() throws RPCException {

        return database.getDatabaseInstanceServer().getAddress();
    }

    @Override
    public int getChordPort() throws RPCException {

        return database.getRemoteInterface().getChordPort();
    }
}
