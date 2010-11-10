/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.manager.recovery.SystemTableAccessException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * Proxy class exposed via RMI, allowing semi-parsed queries to be sent to remote replicas for execution.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstance implements DatabaseInstanceRemote {

    /**
     * The parsed JDBC connection string for this database.
     */
    private final DatabaseURL databaseURL;

    /**
     * Used to parse queries on this machine.
     */
    private final Parser parser;

    /**
     * Whether the database instance is alive or in the process of being shut down.
     */
    private boolean alive = true;

    private final Database database;

    public DatabaseInstance(final DatabaseURL databaseURL, final Session session) {

        this.databaseURL = databaseURL;
        database = session.getDatabase();
        parser = new Parser(session, true);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.dm.DatabaseInstanceRemote#executeUpdate(org.h2.command .Prepared)
     */
    @Override
    public int execute(final String query, final String transactionName, final boolean commitOperation) throws RemoteException, SQLException {

        if (query == null) {
            ErrorHandling.hardError("Shouldn't happen.");
        }

        final Command command = parser.prepareCommand(query);

        try {
            if (commitOperation) {
                return command.update(); // This is a COMMIT.
            }
            else {
                /*
                 * If called from here executeUpdate should always be told the query is part of a larger transaction, because it was
                 * remotely initiated and consequently needs to wait for the remote machine to commit.
                 */

                command.executeUpdate(true);
                return prepare(transactionName); // This wasn't a COMMIT. Execute a PREPARE.
            }
        }
        finally {
            command.close();
        }
    }

    @Override
    public int prepare(final String transactionName) throws RemoteException, SQLException {

        final Command command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
        return command.executeUpdate();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.DatabaseInstanceRemote#getConnectionString()
     */
    @Override
    public String getConnectionString() throws RemoteException {

        return databaseURL.getOriginalURL();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#getLocation()
     */
    @Override
    public DatabaseURL getURL() throws RemoteException {

        return databaseURL;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#getSystemTableLocation()
     */
    @Override
    public DatabaseURL getSystemTableURL() throws RemoteException {

        final DatabaseURL systemTableURL = database.getSystemTableReference().getSystemTableURL();
        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Responding to request for System Table location at database '" + database.getDatabaseLocation() + "'. " + "System table location: " + systemTableURL);

        return systemTableURL;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#executeUpdate(java.lang .String)
     */
    @Override
    public int executeUpdate(final String sql, final boolean systemTableCommand) throws RemoteException, SQLException {

        if (!database.isRunning()) { throw new SQLException("The database either hasn't fully started, or is being shut down."); }

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

    @Override
    public SystemTableRemote recreateSystemTable() throws RemoteException, SystemTableAccessException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Responding to request to recreate System Table on '" + database.getDatabaseLocation() + "'.");

        final ISystemTableReference systemTableReference = database.getSystemTableReference();
        return systemTableReference.migrateSystemTableToLocalInstance(true, true);
    }

    @Override
    public boolean recreateTableManager(final TableInfo tableInfo, final DatabaseURL previousLocation) throws RemoteException {

        boolean success = false;
        try {
            executeUpdate("RECREATE TABLEMANAGER " + tableInfo.getFullTableName() + " FROM '" + previousLocation.sanitizedLocation() + "';", true);
            success = true;
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
        return success;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#setSystemTableLocation
     * (uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
     */
    @Override
    public void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseURL databaseURL) throws RemoteException {

        database.getSystemTableReference().setSystemTableLocation(systemTableLocation, databaseURL);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#findTableManagerReference (org.h2.h2o.util.TableInfo)
     */
    @Override
    public TableManagerRemote findTableManagerReference(final TableInfo ti, final boolean searchOnlyCache) throws RemoteException {

        try {
            return database.getSystemTableReference().lookup(ti, true, searchOnlyCache);
        }
        catch (final SQLException e) {
            ErrorHandling.errorNoEvent("Couldn't find Table Manager at this machine. Table Manager needs to be re-instantiated.");
            // TODO allow for re-instantiation at this point.
            return null;
        }
    }

    @Override
    public void setAlive(final boolean alive) {

        this.alive = alive;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.DatabaseInstanceRemote#testAvailability()
     */
    @Override
    public boolean isAlive() throws RemoteException {

        return alive;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {

        final int result = 31 + (databaseURL.getURL() == null ? 0 : databaseURL.getURL().hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final DatabaseInstance other = (DatabaseInstance) obj;
        if (databaseURL.getURL() == null) {
            if (other.databaseURL.getURL() != null) { return false; }
        }
        else if (!databaseURL.getURL().equals(other.databaseURL.getURL())) { return false; }
        return true;
    }

    @Override
    public boolean isSystemTable() throws RemoteException {

        return database.getSystemTableReference().isSystemTableLocal();
    }

    @Override
    public SystemTableRemote getSystemTable() throws RemoteException {

        return database.getSystemTableReference().getLocalSystemTable();
    }

}
