/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.DatabaseInstance;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.SystemTableReference;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.replication.MetaDataReplicaManager;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.impl.ChordNodeFactory;
import uk.ac.standrews.cs.stachord.interfaces.IChordNode;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * <p>
 * Methods defined in IChordInterface relate to the database's interface and interactions with Chord.
 * 
 * <p>
 * Methods defined in IDatabaseRemote represent the rest of the databases distributed state such as remote references to the local
 * databases, database lookup capabilities.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordRemote implements IDatabaseRemote, IChordInterface, Observer {

    /**
     * The remote interface of the local database instance.
     */
    private DatabaseInstanceRemote localInstance;

    /**
     * Location information for the local database and chord instance.
     */
    private final DatabaseURL localMachineLocation;

    /**
     * Local wrapper for the System Table.
     */
    private final ISystemTableReference systemTableRef;

    /**
     * Name under which the local database instance is bound to its RMI registry.
     */
    private static final String LOCAL_DATABASE_INSTANCE = "LOCAL_INSTANCE";

    /**
     * The local chord node for this database instance.
     */
    private IChordNode chordNode;

    /**
     * The port on which the local Chord node is running its RMI server. This value should be the same as localMachineLocation.getRMIPort();
     */
    private int rmiPort;

    /**
     * Used to cache the location of the System Table by asking the known node where it is on startup. This is only ever really used for
     * this initial lookup. The rest of the System Table funcitonality is hidden behind the SystemTableReference object.
     */
    private DatabaseURL actualSystemTableLocation = null;

    /**
     * This chord nodes predecessor in the ring. When the predecessor changes this is used to determine if the System Table was located on
     * the old predecessor, and to check whether it has failed.
     */
    private IChordRemoteReference predecessor;

    private boolean inShutdown = false;

    private H2OLocatorInterface locatorInterface;

    private Settings databaseSettings;

    private MetaDataReplicaManager metaDataReplicaManager = null;

    private DatabaseURL predecessorURL;

    /**
     * Port to be used for the next database instance. Currently used for testing.
     */
    public static int currentPort = 30000;

    public ChordRemote(final DatabaseURL localMachineLocation, final ISystemTableReference systemTableRef) {

        this.systemTableRef = systemTableRef;
        this.localMachineLocation = localMachineLocation;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.IRemoteDatabase#connectToDatabaseSystem(org.h2.h2o.util. DatabaseURL, org.h2.engine.Session)
     */
    @Override
    public DatabaseURL connectToDatabaseSystem(final Session session, final Settings databaseSettings) throws StartupException {

        this.databaseSettings = databaseSettings;

        establishChordConnection(localMachineLocation, session);

        localMachineLocation.setRMIPort(getRmiPort()); // set the port on
                                                       // which the RMI
                                                       // server is
                                                       // running.

        /*
         * The System Table location must be known at this point, otherwise the database instance will not start.
         */
        if (systemTableRef.getSystemTableURL() == null) {
            ErrorHandling.hardError("System Table not known. This can be fixed by creating a known hosts file (called " + localMachineLocation.sanitizedLocation() + ".instances.properties) and adding the location of a known host.");
        }

        return systemTableRef.getSystemTableURL();
    }

    /**
     * Attempt to establish a new Chord connection by trying to connect to a number of known hosts.
     * 
     * If no established ring is found a new Chord ring will be created.
     * 
     * @param databaseSettings
     */
    private DatabaseURL establishChordConnection(final DatabaseURL localMachineLocation, final Session session) throws StartupException {

        boolean connected = false;

        int attempts = 1; // attempts at connected

        DatabaseURL newSMLocation = null;

        /*
         * Contact descriptor for SM locations.
         */

        /*
         * Try to connect repeatedly until successful. There is a back-off mechanism to ensure this doesn't fail repeatedly in a short space
         * of time.
         */

        List<String> databaseInstances = null;

        final int maxNumberOfAttempts = Integer.parseInt(databaseSettings.get("ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM"));

        while (!connected && attempts < maxNumberOfAttempts) {
            try {
                databaseInstances = locatorInterface.getLocations();
            }
            catch (final Exception e) {
                e.printStackTrace();
                throw new StartupException(e.getMessage());
            }

            /*
             * If this is the first time DB to be run the set of DB instance will be empty and this node should become the schema manager.
             * If there is a list of DB instances this instance should attempt to connect to one of them (but not to itself). If none exist
             * but for itself then it can start as the schema manager. If none exist and it isn't on the list either, just shutdown the
             * database.
             */

            if (databaseInstances != null && databaseInstances.size() > 0) {
                /*
                 * There may be a number of database instances already in the ring. Try to connect.
                 */
                connected = attemptToJoinChordRing(databaseSettings.getLocalSettings(), localMachineLocation, databaseInstances);
            }

            if (connected) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Successfully connected to existing chord ring.");
            }
            else {
                /*
                 * Check whether the local machines URL is included on the list of possible schema managers.
                 */
                boolean localMachineIncluded = false;
                if (databaseInstances != null) {
                    for (final String instance : databaseInstances) {
                        if (instance.contains(localMachineLocation.getURL())) {
                            localMachineIncluded = true;
                            break;
                        }
                    }
                }

                if (!connected && (databaseInstances == null || databaseInstances.size() == 0 || localMachineIncluded)) {
                    /*
                     * Either because there are no known hosts, or because none are still alive. Create a new chord ring.
                     */

                    // Obtain a lock on the locator server first.

                    final LocalH2OProperties localSettings = databaseSettings.getLocalSettings();

                    boolean locked = false;
                    try {
                        locked = locatorInterface.lockLocators(this.localMachineLocation.getDbLocation());
                    }
                    catch (final IOException e) {
                        throw new StartupException("Couldn't obtain a lock to create a new System Table. " + "An IO Exception was thrown trying to contact the locator server (" + e.getMessage() + ").");
                    }

                    if (locked) {
                        final String chordPort = localSettings.getProperty("chordPort");

                        int portToUse = currentPort++;
                        if (chordPort != null) {
                            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Obtained chord port from disk: " + chordPort);
                            portToUse = Integer.parseInt(chordPort);
                        }

                        connected = startChordRing(localMachineLocation.getHostname(), portToUse, localMachineLocation);

                        if (connected) {
                            localSettings.setProperty("chordPort", portToUse + "");
                            localSettings.saveAndClose();
                        }

                        newSMLocation = localMachineLocation;
                        newSMLocation.setRMIPort(portToUse);

                        systemTableRef.setSystemTableURL(newSMLocation);

                        // if (!connected){ //if STILL not connected.
                        // unlockLocator();
                        // throw new
                        // StartupException("Tried to connect to an existing database system and couldn't. Also tried to create"
                        // +
                        // " a new network and this also failed.");
                        // }
                    }
                }

                if (!connected) {
                    /*
                     * Back-off then try to connect again up to n times. If this fails, throw an exception.
                     */

                    final Random r = new Random();
                    try {
                        final int backoffTime = (1000 + r.nextInt(100) * 10) * attempts;
                        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Trying to connect to Chord ring. Attempt number " + attempts + " of " + databaseSettings.get("ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM") + ". Instance at " + localMachineLocation + " is about to back-off for " + backoffTime + " ms.");

                        Thread.sleep(backoffTime);
                    }
                    catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                    attempts++;
                }
            }
        }

        /*
         * If still not connected after many attempts, throw an exception.
         */
        if (!connected) {
            final StringBuilder instances = new StringBuilder();
            for (final String instance : databaseInstances) {
                instances.append(instance + "\n");
            }

            throw new StartupException("\n\nAfter " + attempts + " the H2O instance at " + localMachineLocation + " couldn't find an active instance with System Table state, so it cannot connect to the database system.\n\n" + "Please re-instantiate one of the following database instances:\n\n" + instances + "\n\n");
        }
        else {
            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Database at " + localMachineLocation + " successful created/connected to chord ring.");
        }

        try {
            final DatabaseURL dbURL = systemTableRef.getSystemTableURL();

            if (dbURL == null) {
                systemTableRef.setSystemTableURL(getSystemTableLocation());
            }
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }

        /*
         * Create the local database instance remote interface and register it. This must be done before meta-records are executed.
         */
        localInstance = new DatabaseInstance(localMachineLocation, session);

        exportConnectionObject();

        assert systemTableRef.getSystemTableURL() != null;

        return systemTableRef.getSystemTableURL();
    }

    /**
     * Get a reference to the locator servers for this database system.
     * 
     * @param localDatabaseProperties
     *            Properties file containing the location of the database descriptor.
     * @return
     * @throws StartupException
     *             Thrown if the descriptor file couldn't be found.
     */
    public H2OLocatorInterface getLocatorServerReference(final LocalH2OProperties localDatabaseProperties) throws StartupException {

        final String descriptorLocation = localDatabaseProperties.getProperty("descriptor");
        final String databaseName = localDatabaseProperties.getProperty("databaseName");

        if (descriptorLocation == null || databaseName == null) { throw new StartupException("The location of the database descriptor was not specified. The database will now exit."); }

        try {
            locatorInterface = new H2OLocatorInterface(databaseName, descriptorLocation);
        }
        catch (final IOException e) {
            throw new StartupException(e.getMessage());
        }
        return locatorInterface;
    }

    /**
     * Try to join an existing chord ring.
     * 
     * @return True if a connection was successful; otherwise false.
     */
    private boolean attemptToJoinChordRing(final LocalH2OProperties persistedInstanceInformation, final DatabaseURL localMachineLocation, final List<String> databaseInstances) {

        /*
         * Try to connect via each of the database instances that are known.
         */
        for (final String url : databaseInstances) {
            final DatabaseURL instanceURL = DatabaseURL.parseURL(url);

            /*
             * Check first that the location isn't the local database instance (currently running).
             */
            if (instanceURL.equals(localMachineLocation)) {
                continue;
            }

            // Attempt to connect to a Chord node at this location.
            final String chordPort = persistedInstanceInformation.getProperty("chordPort");

            int portToJoinOn = 0;
            if (chordPort != null) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Obtained chord port from disk: " + chordPort);
                portToJoinOn = Integer.parseInt(chordPort);
            }
            else {
                portToJoinOn = currentPort++;
            }

            if (instanceURL.getRMIPort() == portToJoinOn) {
                portToJoinOn++;
            }

            final boolean connected = joinChordRing(localMachineLocation.getHostname(), portToJoinOn, instanceURL.getHostname(), instanceURL.getRMIPort(), localMachineLocation.sanitizedLocation());

            if (connected) {
                persistedInstanceInformation.setProperty("chordPort", rmiPort + "");
                persistedInstanceInformation.saveAndClose();
                chordNode.addObserver(this);

                Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Successfully connected to an existing chord ring at " + url);
                return true;
            }
        }

        return false;
    }

    /**
     * Export the local database instance remote using UnicastRemoteObject.exportObject, so that it is remotely accessible.
     */
    private void exportConnectionObject() {

        /*
         * This is done so that the local database instance is exported correctly on RMI. It doesn't seem to work properly otherwise ('No
         * such object' errors in Database.createH2OTables()).
         */
        DatabaseInstanceRemote stub = null;
        try {
            stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(localInstance, 0);
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }
        try {

            getLocalRegistry().rebind(LOCAL_DATABASE_INSTANCE, stub);

            localInstance = stub;
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.IRemoteDatabase#getLocalDatabaseInstance()
     */
    @Override
    public DatabaseInstanceRemote getLocalDatabaseInstance() {

        return localInstance;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.IDatabaseRemote#getLocalMachineLocation()
     */
    @Override
    public DatabaseURL getLocalMachineLocation() {

        return localMachineLocation;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#getDatabaseInstanceAt(uk.ac.standrews .cs.stachordRMI.interfaces.IChordRemoteReference)
     */
    @Override
    public DatabaseInstanceRemote getDatabaseInstanceAt(final IChordRemoteReference lookupLocation) throws RemoteException {

        final String hostname = lookupLocation.getRemote().getAddress().getHostName();

        final int port = lookupLocation.getRemote().getAddress().getPort();

        try {
            return getDatabaseInstanceAt(hostname, port);
        }
        catch (final NotBoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#getDatabaseInstanceAt(org.h2.h2o.util .DatabaseURL)
     */
    @Override
    public DatabaseInstanceRemote getDatabaseInstanceAt(final DatabaseURL dbURL) throws RemoteException {

        if (dbURL.equals(localMachineLocation)) { return getLocalDatabaseInstance(); }

        try {
            return getDatabaseInstanceAt(dbURL.getHostname(), dbURL.getRMIPort());
        }
        catch (final NotBoundException e) {
            ErrorHandling.errorNoEvent("Local instance of database " + dbURL + " not bound at " + dbURL.getRMIPort() + "." + " Request made by " + localMachineLocation.getURLwithRMIPort());
            e.printStackTrace();
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#getDatabaseInstanceAt(java.lang.String, int)
     */
    @Override
    public DatabaseInstanceRemote getDatabaseInstanceAt(final String hostname, final int port) throws RemoteException, NotBoundException {

        DatabaseInstanceRemote dir = null;

        final Registry remoteRegistry = LocateRegistry.getRegistry(hostname, port);

        dir = (DatabaseInstanceRemote) remoteRegistry.lookup(LOCAL_DATABASE_INSTANCE);

        return dir;
    }

    /**
     * Start a new Chord ring at the specified location.
     * 
     * @param hostname
     *            The hostname on which the Chord ring will be started. This must be a local address to the machine on which this process is
     *            running.
     * @param port
     *            The port on which the Chord node will listen. This is port on which the RMI registry will be created.
     * @param databaseName
     *            The name of the database instance starting this Chord ring. This information is used purely for diagnostic output, so can
     *            be left null.
     * @return True if the chord ring was started successfully; otherwise false.
     */
    private boolean startChordRing(final String hostname, final int port, final DatabaseURL databaseURL) {

        rmiPort = port;

        final InetSocketAddress localChordAddress = new InetSocketAddress(hostname, port);
        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Deploying new Chord ring on " + hostname + ":" + port);

        /*
         * Create a new Chord Ring.
         */
        try {
            chordNode = ChordNodeFactory.createLocalNode(localChordAddress);

            if (Constants.IS_TEST) {
                // allNodes.add(chordNode);
            }
        }
        catch (final RemoteException e) {
            e.printStackTrace();
            return false;
        }

        systemTableRef.setLookupLocation(chordNode.getSelfReference());

        actualSystemTableLocation = databaseURL;

        systemTableRef.setInKeyRange(true);

        chordNode.addObserver(this);

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Started local Chord node on : " + databaseURL.sanitizedLocation() + " : " + hostname + ":" + port + " : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : System Table at " + systemTableRef.getLookupLocation() + " : ");
        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT,
        // "System Table key: : : : :" +
        // SystemTableReference.systemTableKey.toString(10) + " : " +
        // SystemTableReference.systemTableKey);

        return true;
    }

    /**
     * Join an existing chord ring.
     * 
     * @param localHostname
     *            The hostname on which this node will start. This must be a local address to the machine on which this process is running.
     * @param localPort
     *            The port on which this node will listen. The RMI server will run on this port.
     * @param remoteHostname
     *            The hostname of a known host in the existing Chord ring.
     * @param remotePort
     *            The port on which a known host is listening.
     * @param databaseName
     *            The name of the database instance starting this Chord ring. This information is used purely for diagnostic output, so can
     *            be left null.
     * @return true if a node was successfully created and joined an existing Chord ring; otherwise false.
     */
    private boolean joinChordRing(final String localHostname, final int localPort, final String remoteHostname, final int remotePort, final String databaseName) {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Trying to connect to existing Chord ring on " + remoteHostname + ":" + remotePort);

        rmiPort = localPort;

        InetSocketAddress localChordAddress = new InetSocketAddress(localHostname, rmiPort);
        final InetSocketAddress knownHostAddress = new InetSocketAddress(remoteHostname, remotePort);

        boolean connected = false;
        int attempts = 0;

        final int maxNumberOfAttempts = Integer.parseInt(databaseSettings.get("ATTEMPTS_AFTER_BIND_EXCEPTIONS"));

        while (!connected && attempts < maxNumberOfAttempts) { // only have
                                                               // multiple
                                                               // attempts if
                                                               // there is a
                                                               // bind
                                                               // exception.
                                                               // otherwise
                                                               // this just
                                                               // returns
                                                               // false.
            try {
                chordNode = ChordNodeFactory.createLocalNode(localChordAddress);
                chordNode.join(ChordNodeFactory.bindToRemoteNode(knownHostAddress));

            }
            catch (final ConnectException e) { // database instance we're trying to
                                               // connect to doesn't exist.
                // e.printStackTrace();
                ErrorHandling.errorNoEvent("Failed to connect to chord node on + " + localHostname + ":" + rmiPort + " known host: " + remoteHostname + ":" + remotePort);
                return false;
            }
            catch (final ExportException e) { // bind exception (most commonly
                // nested in ExportException

                if (attempts > 50) {
                    ErrorHandling.errorNoEvent("Failed to connect to chord ring with known host: " + remoteHostname + ":" + remotePort + ", on address " + localHostname + ":" + rmiPort + ".");
                }
                connected = false;
            }
            catch (final NotBoundException e) {
                ErrorHandling.errorNoEvent("Failed to create new chord node on + " + localHostname + ":" + rmiPort + " known host: " + remoteHostname + ":" + remotePort);
                connected = false;
            }
            catch (final RemoteException e) {
                e.printStackTrace();
                return false;
            }

            if (chordNode != null) {
                connected = true;
            }
            if (!connected) {
                localChordAddress = new InetSocketAddress(localHostname, rmiPort++);
            }

            attempts++;
        }

        if (!connected) { return false; }

        systemTableRef.setInKeyRange(false);

        rmiPort = localChordAddress.getPort();

        try {
            final DatabaseInstanceRemote lookupInstance = getDatabaseInstanceAt(remoteHostname, remotePort);
            actualSystemTableLocation = lookupInstance.getSystemTableURL();
            systemTableRef.setSystemTableURL(actualSystemTableLocation);
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }
        catch (final NotBoundException e) {
            e.printStackTrace();
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Started local Chord node on : " + databaseName + " : " + localHostname + " : " + rmiPort + " : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : System Table at " + systemTableRef.getLookupLocation() + " : " + chordNode.getSuccessor().getKey());

        return true;
    }

    /**
     * Called by various chord functions in {@link ChordNodeImpl} which are being observed. Of particular interest to this class is the case
     * where the predecessor of a node changes. This is used to assess whether the System Tables location has changed.
     * 
     * <p>
     * If changing this method please note that it is called synchronously by the Observable class, ChordNodeImpl. This means that if you
     * try and do something such as chordNode.stabilize() you will possibly introduce some form of deadlock into Chord. This is difficult to
     * debug, but is the most likely cause of a ring failing to close properly (i.e. not stablizing even after an extended period).
     * 
     * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
     */
    @Override
    public void update(final Observable o, final Object arg) {

        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, arg);

        /*
         * If the predecessor of this node has changed.
         */
        if (arg.equals(IChordNode.PREDECESSOR_CHANGE_EVENT)) {
            predecessorChangeEvent();
        }
        else if (arg.equals(IChordNode.SUCCESSOR_CHANGE_EVENT)) {
            successorChangeEvent();
        }
    }

    /**
     * Called when the successor has changed. Used to check whether the System Table was on the predecessor, and if it was (and has failed)
     * restart the System Table elsewhere.
     */
    private void predecessorChangeEvent() {

        try {
            if (predecessor == null) {
                predecessor = chordNode.getPredecessor();
                return;
            }
            predecessor.getRemote().getPredecessor();
            return; // the old predecessor has not failed, so nothing needs to
                    // be recovered.
        }
        catch (final RemoteException e1) {
            // If the old predecessor is no longer available it has failed - try
            // to recover processses.
            if (predecessorURL != null) {
                H2OEventBus.publish(new H2OEvent(predecessorURL.getDbLocation(), DatabaseStates.DATABASE_FAILURE, null));
            }
        }

        final boolean systemTableWasOnPredecessor = systemTableRef.isThisSystemTableNode(predecessor);

        predecessor = chordNode.getPredecessor();

        /*
         * This will often be null at this point because it hasn't stabilized.
         */
        if (predecessor != null) {
            try {
                predecessorURL = getDatabaseInstanceAt(predecessor).getURL();
            }
            catch (final RemoteException e) {
                e.printStackTrace();
            }
        }
        else {
            predecessorURL = null;

        }

        boolean systemTableAlive = true;
        ISystemTable newSystemTable = null;
        if (systemTableWasOnPredecessor) {
            systemTableAlive = isSystemTableActive();

            if (!systemTableAlive) {
                try {
                    newSystemTable = systemTableRef.findSystemTable();

                    // Now try to recreate any Table Managers that were on the failed
                    // machine.
                    // recreateTableManagers(oldPredecessorURL);
                    try {
                        newSystemTable.checkTableManagerAccessibility();
                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
                }
                catch (final SQLException e) {
                    // If this fails it doesn't matter at this point.
                }
            }
        }

    }

    private boolean isSystemTableActive() {

        boolean systemTableAlive;
        try {
            systemTableRef.getSystemTable().checkConnection();
            systemTableAlive = true;
        }
        catch (final Exception e) {
            systemTableAlive = false;
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "The System Table is no longer accessible.");
        }
        return systemTableAlive;
    }

    /**
     * The successor has changed. Make sure the System Table is replicated to the new successor if this instance is controlling the schema
     * manager.
     */
    private void successorChangeEvent() {

        if (Constants.IS_NON_SM_TEST) { return; // Don't do this if we're testing something that isn't to do with this replication.
        }

        /*
         * Check whether there are any table managers running locally.
         */
        Set<TableManagerWrapper> localTableManagers = null;
        try {
            /*
             * This will throw a NullPointerException if the older successor has failed and was the System Table.
             */

            final SystemTableRemote systemTable = systemTableRef.getSystemTable();

            if (systemTable == null) {
                /*
                 * The previous successor has failed and it was the System Table, so no System Table exists currently. It is not the
                 * responsibility of this node (the System Table's predecessor) to restart the System Table.
                 */
            }
            else {
                localTableManagers = systemTableRef.getSystemTable().getLocalDatabaseInstances(localMachineLocation);
            }
        }
        catch (final RemoteException e) {
            ErrorHandling.errorNoEvent("Remote exception thrown. Happens when successor has very recently changed and chord ring hasn't stabilized.");
        }
        catch (final MovedException e) {
            try {
                systemTableRef.handleMovedException(e);
            }
            catch (final SQLException e1) {
                e1.printStackTrace();
            }
        }

        final IChordRemoteReference successor = chordNode.getSuccessor();

        /*
         * If table managers running locally or the System Table is located locally then get a reference to the suceessor instance so that
         * we can replicate meta-data onto it. If not, don't go to the effort of looking up the successor.
         */
        try {

            String hostname = null;
            int port = 0;

            hostname = successor.getRemote().getAddress().getHostName();

            port = successor.getRemote().getAddress().getPort();

            DatabaseInstanceRemote successorInstance = null;

            successorInstance = getDatabaseInstanceAt(hostname, port);

            if (systemTableRef.isSystemTableLocal() || localTableManagers != null && localTableManagers.size() > 0) {

                final DatabaseInstanceWrapper successorInstanceWrapper = new DatabaseInstanceWrapper(successorInstance.getURL(), successorInstance, true);

                metaDataReplicaManager.replicateMetaDataIfPossible(systemTableRef, true, successorInstanceWrapper);

            }

            /*
             * Now do the same thing for table manager replication.
             */

            if (localTableManagers != null && metaDataReplicaManager != null) {

                // delete query must remove entries for all table managers
                // replicated on this machine.

                final DatabaseInstanceWrapper successorInstanceWrapper = new DatabaseInstanceWrapper(successorInstance.getURL(), successorInstance, true);

                metaDataReplicaManager.replicateMetaDataIfPossible(systemTableRef, false, successorInstanceWrapper);

            }
        }
        catch (final RemoteException e) {
            ErrorHandling.errorNoEvent("Successor not known: " + e.getMessage());
        }
        catch (final NotBoundException e) {
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#getRmiPort()
     */
    @Override
    public int getRmiPort() {

        return rmiPort;
    }

    /**
     * Returns a reference to this chord nodes RMI registry.
     * 
     * @return The RMI registry of this chord node.
     * @throws RemoteException
     */
    private Registry getLocalRegistry() throws RemoteException {

        return LocateRegistry.getRegistry(rmiPort);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#shutdown()
     */
    @Override
    public void shutdown() {

        if (inShutdown) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Chord node is already shutting down: " + chordNode);
            return;
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Shutting down node: " + chordNode);

        inShutdown = true;

        if (chordNode == null) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Chord node was not initialized so the system is shutting down without transferring any active tables or managers.");
            return;
        }

        final IChordRemoteReference successor = chordNode.getSuccessor();

        final boolean successesorIsDifferentMachine = successor != null && !chordNode.getKey().equals(successor.getKey());
        final boolean thisIsntATestThatShouldPreventThis = !Constants.IS_NON_SM_TEST && !Constants.IS_TEAR_DOWN;
        final boolean systemTableHeldLocally = systemTableRef.isSystemTableLocal();

        DatabaseInstanceRemote successorDB = null;

        if (successesorIsDifferentMachine && thisIsntATestThatShouldPreventThis) {

            try {
                successorDB = getDatabaseInstanceAt(successor);
            }
            catch (final RemoteException e1) {
                e1.printStackTrace();
            }
        }

        /*
         * Migrate any local Table Managers.
         */
        if (successesorIsDifferentMachine && thisIsntATestThatShouldPreventThis) {

            try {
                // /successorDB = getDatabaseInstanceAt(successor);

                final Set<TableManagerWrapper> localManagers = systemTableRef.getSystemTable().getLocalDatabaseInstances(getLocalMachineLocation());

                /*
                 * Create replicas if needed.
                 */
                for (final TableManagerWrapper wrapper : localManagers) {

                    final TableManagerRemote dmr = wrapper.getTableManager();
                    if (dmr.getReplicaManager().contains(new DatabaseInstanceWrapper(localMachineLocation, localInstance, true)) && dmr.getReplicaManager().getNumberOfReplicas() == 1) {
                        // This machine holds the only replica - replicate on
                        // the successor as well.
                        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Replicating table [" + wrapper.getTableInfo().getFullTableName() + "] to successor: " + successor);

                        successorDB.executeUpdate("CREATE REPLICA " + wrapper.getTableInfo().getFullTableName() + ";", false);
                    }
                }

                /*
                 * Migrate Table Managers.
                 */
                for (final TableManagerWrapper wrapper : localManagers) {

                    Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Migrating Table Manager [" + wrapper.getTableInfo().getFullTableName() + "] to successor: " + successor);

                    successorDB.executeUpdate("MIGRATE TABLEMANAGER " + wrapper.getTableInfo().getFullTableName(), false);

                }

            }
            catch (final RemoteException e) {
                ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
            }
            catch (final MovedException e) {
                ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
            }
            catch (final SQLException e) {
                ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
            }
        }

        /*
         * Migrate the System Table if needed.
         */
        if (systemTableHeldLocally && successesorIsDifferentMachine && thisIsntATestThatShouldPreventThis) {

            // Migrate the System Table to this node before shutdown.
            try {
                Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Migrating System Table to successor: " + successor);
                successorDB = getDatabaseInstanceAt(successor);

                successorDB.executeUpdate("MIGRATE SYSTEMTABLE", false);

            }
            catch (final Exception e) {
                ErrorHandling.errorNoEvent("Failed to migrate System Table to successor: " + successor);
            }
        }

        if (!Constants.IS_NON_SM_TEST) {
            chordNode.shutDown();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#getChordNode()
     */
    @Override
    public IChordNode getChordNode() {

        return chordNode;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#getSystemTableLocation()
     */
    @Override
    public DatabaseURL getSystemTableLocation() throws RemoteException {

        if (actualSystemTableLocation != null) { return actualSystemTableLocation; }

        IChordRemoteReference lookupLocation = null;
        lookupLocation = lookupSystemTableNodeLocation();
        systemTableRef.setLookupLocation(lookupLocation);

        final String lookupHostname = lookupLocation.getRemote().getAddress().getHostName();
        final int lookupPort = lookupLocation.getRemote().getAddress().getPort();

        DatabaseInstanceRemote lookupInstance;
        try {
            lookupInstance = getDatabaseInstanceAt(lookupHostname, lookupPort);

            actualSystemTableLocation = lookupInstance.getSystemTableURL();
            systemTableRef.setSystemTableURL(actualSystemTableLocation);
        }
        catch (final NotBoundException e) {
            e.printStackTrace();
        }
        return actualSystemTableLocation;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#lookupSystemTableNodeLocation()
     */
    @Override
    public IChordRemoteReference lookupSystemTableNodeLocation() throws RemoteException {

        IChordRemoteReference lookupLocation = null;

        if (chordNode != null) {
            lookupLocation = chordNode.lookup(SystemTableReference.systemTableKey);
        }

        return lookupLocation;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#exportSystemTable(org.h2.h2o.manager .SystemTableReference)
     */
    @Override
    public void exportSystemTable(final ISystemTableReference systemTableRef) {

        ISystemTable stub = null;

        try {
            stub = (ISystemTable) UnicastRemoteObject.exportObject(systemTableRef.getSystemTable(), 0);
            getLocalRegistry().bind(SystemTableReference.SCHEMA_MANAGER, stub);

        }
        catch (final Exception e) {
            e.printStackTrace();
            // ErrorHandling.hardError("Failed to export and bind System Table.");
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#getLocalChordReference()
     */
    @Override
    public IChordRemoteReference getLocalChordReference() {

        return chordNode.getSelfReference();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#getLookupLocation(uk.ac.standrews.cs .nds.p2p.interfaces.IKey)
     */
    @Override
    public IChordRemoteReference getLookupLocation(final IKey systemTableKey) throws RemoteException {

        return chordNode.lookup(systemTableKey);

    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IDatabaseRemote#inShutdown()
     */
    @Override
    public boolean inShutdown() {

        return inShutdown;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.remote.IChordInterface#bind(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote)
     */
    @Override
    public void bind(final String fullTableName, final TableManagerRemote stub) {

        try {
            getLocalRegistry().rebind(fullTableName, stub);
        }
        catch (final Exception e) {
            // Doesn't matter.
        }
    }

    /**
     * Called when the local database has been created, has started an ST, and is ready to receive requests.
     * 
     * <p>
     * The system will start throwing errors about meta-tables not existing if this is called too soon.
     */
    public void commitSystemTableCreation() {

        boolean successful = false;

        try {
            successful = locatorInterface.commitLocators(localMachineLocation.getDbLocation());
        }
        catch (final Exception e) {
            e.printStackTrace();
            successful = false;
        }

        if (!successful) {
            ErrorHandling.errorNoEvent("Failed to unlock database locator servers after creating the system table.");
        }
    }

    /**
     * Called when the database is ready to replicate meta-data (i.e. it has created the local H2O meta-data tables at this point.
     * 
     * <p>
     * This is called by the database object at the end of startup, so it is limited in what it can do. Anything involving querying the
     * local database may have to be run asynchronously.
     * 
     * @param metaDataReplicaManager
     *            The replica manager for this databases meta-data.
     */
    public void setAsReadyToReplicateMetaData(final MetaDataReplicaManager metaDataReplicaManager) {

        this.metaDataReplicaManager = metaDataReplicaManager;
    }

    @Override
    public H2OLocatorInterface getLocatorInterface() {

        /*
         * Obtain a reference to the locator servers if one is not already held.
         */
        if (locatorInterface == null) {
            final LocalH2OProperties persistedInstanceInformation = new LocalH2OProperties(localMachineLocation);
            try {
                persistedInstanceInformation.loadProperties();
            }
            catch (final IOException e) {
                e.printStackTrace();
            }
            try {
                locatorInterface = getLocatorServerReference(persistedInstanceInformation);
            }
            catch (final StartupException e) {
                ErrorHandling.errorNoEvent("Failed to obtain a reference to the locator servers: " + e.getMessage());
            }
        }

        return locatorInterface;
    }

}
