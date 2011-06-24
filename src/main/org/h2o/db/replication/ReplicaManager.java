/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.replication;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Stores the location of each replica for a give table, including the update ID for each of these replicas (stating the last time a replica
 * was updated), and the set of replicas that are currently active (i.e. up-to-date).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaManager {

    private static final long serialVersionUID = 6064010173578943054L;

    /**
     * Set of databases which hold replicas for this table. All replica locations are held here.
     * 
     * <p>
     * Key: Location of the replica
     * <p>
     * Value: Number given to the last update made at that replica.
     */
    private final Map<DatabaseInstanceWrapper, Integer> allReplicas;

    /**
     * The set of replicas that are currently active - i.e. up-to-date. Queries can only be executed on this set of replicas.
     */
    private Map<DatabaseInstanceWrapper, Integer> activeReplicas;

    /**
     * The database instance which is running this Table Manager.
     */
    private DatabaseInstanceWrapper primaryLocation;

    public ReplicaManager() {

        allReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
        activeReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
        primaryLocation = null;

    }

    /**
     * Used to recreate the state of the replica manager from an existing replica manager on another machine.
     * @param activeReplicas2
     * @param allReplicas2
     * @param primaryLocation
     */
    public ReplicaManager(final Map<DatabaseInstanceWrapper, Integer> activeReplicas, final Map<DatabaseInstanceWrapper, Integer> allReplicas, final DatabaseInstanceWrapper primaryLocation) {

        this.activeReplicas = activeReplicas;
        this.allReplicas = allReplicas;
        this.primaryLocation = primaryLocation;

    }

    /**
     * Used to recreate the state of the replica manager from an existing replica manager on another machine.
     * @param otherTableManager An existing table manager on another machine.
     */
    public static ReplicaManager recreateReplicaManager(final ITableManagerRemote otherTableManager) throws RPCException, MovedException {

        return new ReplicaManager(otherTableManager.getActiveReplicas(), otherTableManager.getAllReplicas(), otherTableManager.getDatabaseLocation());
    }

    /**
     * Add a new replica to the active set of replicas. It will be given the latest update ID recorded.
     * 
     * @param replicaLocation
     */
    public void add(final DatabaseInstanceWrapper replicaLocation) {

        assert replicaLocation != null;

        if (primaryLocation == null) {
            primaryLocation = replicaLocation;
        }

        addToAllReplicas(replicaLocation, getCurrentUpdateID());
        activeReplicas.put(replicaLocation, getCurrentUpdateID());
    }

    private Integer addToAllReplicas(final DatabaseInstanceWrapper replicaLocation, final Integer newUpdateID) {

        return getAllReplicas().put(replicaLocation, newUpdateID);
    }

    public int getCurrentUpdateID() {

        for (final Integer updateID : activeReplicas.values()) {

            return updateID; // all the update IDs will be the same because all these replicas are active.
        }

        return 0; // will return this for inserts where there are not yet any active replicas.
    }

    /**
     * Add a set of databases to the active set of replicas. This just calls the {@link #add(DatabaseInstanceWrapper)} method on each
     * replica location in the list.
     * 
     * @param replicaLocations
     *            a number of replica locations.
     */
    public void add(final List<DatabaseInstanceWrapper> replicaLocations) {

        assert replicaLocations != null;

        if (replicaLocations.size() == 0) { return; }

        for (final DatabaseInstanceWrapper diw : replicaLocations) {
            add(diw);
        }
    }

    /**
     * @return The number of replicas for this table. Some may not currently be up-to-date.
     */
    public int allReplicasSize() {

        return getAllReplicas().size();
    }

    /**
     * @return
     */
    public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() {

        return activeReplicas;
    }

    /**
     * Remove a database instance as a location of a stored replica.
     * 
     * @param createFullDatabaseLocation
     */
    public void remove(final DatabaseInstanceWrapper dbInstance) {

        getAllReplicas().remove(dbInstance);
        activeReplicas.remove(dbInstance);
    }

    /**
     * Finish an update by revising the set of replica locations with new information on:
     * <p>
     * The updateID of the last update committed on each replica.
     * <p>
     * The set of replicas which are deemed active.
     * 
     * @param commit
     *            True if all replicas are committing.
     * 
     * @param committedQueries
     *            The queries which are to be committed.
     * @param firstPartOfUpdate
     *            True if this is the first part of an update that is executing asynchronously. False if it is one of the later replicas
     *            being committed.
     * @return If this is the first part of the update, this returns the set of replicas that are now inactive. If it is the second part of
     *         the update this returns the replicas that are now active.
     * @throws SQLException 
     */
    public Set<DatabaseInstanceWrapper> completeUpdate(final boolean commit, final Collection<CommitResult> committedQueries, final TableInfo tableInfo, final boolean firstPartOfUpdate) throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL, "commit: " + commit + " table info: " + tableInfo.getFullTableName());

        if (!thisTableWasUpdated(committedQueries, tableInfo)) { return new HashSet<DatabaseInstanceWrapper>(); }
        //Replicas that are currently marked as active (this may be changed during this update).
        final HashMap<DatabaseInstanceWrapper, Integer> oldActiveReplicas = new HashMap<DatabaseInstanceWrapper, Integer>(activeReplicas);

        // queries that are successfully committed in this method (their update ID must be correct.
        final List<CommitResult> successfullyCommittedQueries = new LinkedList<CommitResult>();

        //The database instances that have had their replicas updated by this method.
        final Set<DatabaseInstanceWrapper> instancesUpdated = new HashSet<DatabaseInstanceWrapper>();

        //The update ID that is expected for these updates to pass.
        final int expectedUpdateID = getUpdateIDFromCommittedQueries(committedQueries, tableInfo);

        /*
         * Check whether all updates were rollbacks. If they were there is no need to remove any replicas from the set of active replicas.
         */
        boolean allRollback = false;

        if (!commit) {
            if (committedQueries == null) {
                allRollback = true;
            }
            else {
                for (final CommitResult commitResult : committedQueries) {
                    if (!commitResult.isCommit()) {
                        allRollback = true;
                        break;
                    }
                }
            }
        }

        if (committedQueries != null && committedQueries.size() > 0) {

            if (!allRollback && firstPartOfUpdate) {
                // Reset the active set.
                activeReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
            }

            /*
             * Loop through each replica which was updated, re-adding them into the replicaLocations hashmap along with the new updateID.
             */

            for (final CommitResult commitResult : committedQueries) {

                final DatabaseInstanceWrapper wrapper = commitResult.getDatabaseInstanceWrapper();

                if (instancesUpdated.contains(wrapper)) {
                    continue; // don't update info for the same replica twice.
                }

                /*
                 * Meaning of IF statement: IF this replica location is actually a replica location for this table AND the commit
                 * information is for this table OR no table is specified (which is the case for queries which have bypassed the
                 * asynchronous update manager. THEN... Update the active/all replica set with new update IDs where appropriate.
                 */
                if (allReplicas.containsKey(wrapper) && (tableInfo.equals(commitResult.getTable()) || commitResult.getTable() == null)) {

                    final Integer currentID = allReplicas.get(wrapper);

                    if (expectedUpdateID == currentID) {
                        /*
                         * The updateID of this current replica equals the update ID that was expected of this replica at this point. Commit
                         * can proceed.
                         */

                        final int newUpdateID = currentID + 1;

                        if (commitResult.isCommit()) {
                            instancesUpdated.add(wrapper);

                            activeReplicas.put(wrapper, newUpdateID);
                            addToAllReplicas(wrapper, newUpdateID);

                            successfullyCommittedQueries.add(commitResult); // this query has been successfully updated.
                            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Replica successfully updated: " + commitResult);
                        }
                        else {
                            if (!allRollback) {
                                /*
                                 * Only remove replicas in case of rollback if some of the replicas managed to commit. Otherwise they are
                                 * all still in a consistent state and all still active.
                                 */
                                activeReplicas.remove(wrapper);
                                getAllReplicas().remove(wrapper);
                            }
                        }

                    }
                    else {
                        /*
                         * The update ID of this replica does not match that which was expected. This replica will not commit.
                         */
                        ErrorHandling.errorNoEvent("Replica will not commit because update IDs did not match. Expected: " + expectedUpdateID + "; Actual current: " + currentID);

                        if (allReplicas.size() == 1) {
                            System.out.println("");
                            throw new SQLException("Update IDs don't match on table " + tableInfo + ". There is only one replica so this shouldn't happen (i.e. replication is synchronous). Expected: " + expectedUpdateID + "; Actual current: " + currentID);
                        }
                    }

                } // In many cases it won't contain this key, but another table (part of the same transaction) was on this machine.

            }
        }

        /*
         * We return the set of replicas that have become inactive as a result of this commit, or if this is the asynchronous commit, we
         * return the set of queries that have become active.
         */

        if (firstPartOfUpdate && instancesUpdated.size() > 0) {
            return getInactiveReplicas();
        }
        else if (firstPartOfUpdate && instancesUpdated.size() == 0) {
            // If no replicas were updated make sure that the original set of active replicas is still valid.
            activeReplicas = oldActiveReplicas;
            return getInactiveReplicas();
        }
        else {
            return instancesUpdated;
        }
    }

    /**
     * Checks whether any of this tables replicas were updated.
     * @param committedQueries information on all the updates which occurred in this transaction.
     * @param tableInfo the name of the table for which this replica manager is responsible.
     * @return true if any of this tables replicas were involved in an update.
     */
    private boolean thisTableWasUpdated(final Collection<CommitResult> committedQueries, final TableInfo tableInfo) {

        if (committedQueries == null) { return false; }

        for (final CommitResult cr : committedQueries) {
            try {
                if (cr.getTable() != null && tableInfo != null && cr.getTable().equals(tableInfo)) { return true; }
            }
            catch (final Exception e) {
                e.printStackTrace();
            }
        }

        return false;
    }

    /**
     * Get the set of replicas that are currently inactive.
     * 
     * @return
     */
    private Set<DatabaseInstanceWrapper> getInactiveReplicas() {

        final Set<DatabaseInstanceWrapper> inactiveReplicas = new HashSet<DatabaseInstanceWrapper>();

        for (final DatabaseInstanceWrapper replica : allReplicas.keySet()) {

            if (!activeReplicas.containsKey(replica)) {
                inactiveReplicas.add(replica);
            }

        }

        return inactiveReplicas;
    }

    private int getUpdateIDFromCommittedQueries(final Collection<CommitResult> committedQueries, final TableInfo tableInfo) {

        int updateID = 0;

        if (committedQueries == null) { return 0; }

        for (final CommitResult cr : committedQueries) {
            try {
                if (cr.getExpectedUpdateID() > updateID && cr.getTable() != null && tableInfo != null && cr.getTable().equals(tableInfo)) {
                    updateID = cr.getUpdateID(); // XXX this used to be expected update ID, but was changed because the expected update ID was often too high.
                }
            }
            catch (final Exception e) {
                e.printStackTrace();
            }
        }

        return updateID;
    }

    /**
     * Whether every replica contains the most recent copy of the data.
     * 
     * @return True if every replica is deemed active - false, if some are inactive because they don't contain the latest updates.
     */
    public boolean areReplicasConsistent() {

        return activeReplicas.size() == getAllReplicas().size();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "ReplicaManager [number of replicas=" + getAllReplicas().size() + "]";
    }

    /**
     * 
     */
    public int getNumberOfReplicas() {

        return activeReplicas.size();
    }

    public DatabaseInstanceWrapper getPrimaryLocation() {

        return primaryLocation;
    }

    /**
     * @return
     */
    public String[] getReplicaLocationsAsStrings() {

        final String[] locations = new String[activeReplicas.size()];

        int i = 0;

        locations[i++] = primaryLocation.getURL().getURLwithRMIPort(); // the primary location should always be first.

        for (final DatabaseInstanceWrapper r : activeReplicas.keySet()) {
            if (r.equals(primaryLocation)) {
                continue;
            }
            locations[i++] = r.getURL().getURLwithRMIPort();
        }

        return locations;
    }

    public void remove(final Set<DatabaseInstanceWrapper> failed) {

        for (final DatabaseInstanceWrapper wrapper : failed) {
            activeReplicas.remove(wrapper);
        }
    }

    public boolean contains(final DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException {

        return activeReplicas.containsKey(databaseInstanceWrapper);
    }

    public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() {

        return allReplicas;
    }

    public DatabaseInstanceWrapper getManagerLocation() {

        return primaryLocation;
    }

    public void removeFromActiveSet(final DatabaseID failedMachine) {

        activeReplicas.remove(new DatabaseInstanceWrapper(failedMachine, null, false));
    }
}
