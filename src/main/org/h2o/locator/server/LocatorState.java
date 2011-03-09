/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import org.h2o.locator.messages.LockRequestResponse;
import org.h2o.locator.messages.ReplicaLocationsResponse;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Used to write to database locator file. This class uses readers-writers model.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorState {

    private int activeReaders = 0;

    private boolean writerPresent = false;

    private final File locatorFile;

    private boolean locked = false;

    private String databaseWithLock = null;

    int updateCount = 1;

    public final static int LOCK_TIMEOUT = 10000;

    private long lockCreationTime = 0l;

    protected LocatorState(final String location) {

        locatorFile = new File(location);

        if (locatorFile.getParentFile() != null) {
            final boolean successful = locatorFile.getParentFile().mkdirs();

            if (!successful) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to create folder for locator file. It may already exist.");
            }
        }

        try {
            if (!(locatorFile.createNewFile() || locatorFile.isFile())) {
                ErrorHandling.errorNoEvent("This is a directory, when I file should have been given.");
            }
        }
        catch (final IOException e1) {
            e1.printStackTrace();
        }

    }

    /**
     * Read the set of database locations from the file.
     * 
     * @return Set of db locations which hold system table replicas.
     */
    public ReplicaLocationsResponse readLocationsFromFile() {

        startRead();

        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Reader reading:");

        final List<String> locations = new LinkedList<String>();

        try {
            final BufferedReader input = new BufferedReader(new FileReader(locatorFile));

            try {
                String line = null;

                while ((line = input.readLine()) != null) {
                    locations.add(line);
                }
            }
            finally {
                input.close();
            }

        }
        catch (final Exception e) {
            e.printStackTrace();
        }

        final ReplicaLocationsResponse response = new ReplicaLocationsResponse(locations, updateCount);

        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Finished reading.");
        stopRead();

        return response;
    }

    /**
     * Write the given array of database locations to the locator file.
     * 
     * @param databaseLocations
     *            Locations to be written to the file, each on a new line.
     */
    public boolean writeLocationsToFile(final String[] databaseLocations) {

        startWrite();

        boolean successful = false;

        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Writer writing.");

        try {
            final Writer output = new BufferedWriter(new FileWriter(locatorFile));

            try {
                for (final String location : databaseLocations) {
                    output.write(location + "\n");
                }
                successful = true;
            }

            finally {
                output.close();
            }

        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Finished writing.");
        stopWrite();

        return successful;
    }

    /**
     * Create a lock file. This is used as a mechanism for re-creating System Tables.
     * 
     * @param requestingDatabase
     *            The database which is requesting the lock.
     * @return true if the lock was successfully taken out; otherwise false.
     */
    public LockRequestResponse lock(final String requestingDatabase) {

        startWrite();

        boolean success = false;

        if (locked) {
            // Check that the lock hasn't timed out.
            if (lockCreationTime + LOCK_TIMEOUT < System.currentTimeMillis()) {
                ErrorHandling.errorNoEvent("Lock held by " + databaseWithLock + " has timed out.");
                locked = false;
                databaseWithLock = null;
                lockCreationTime = 0l;
            }
        }

        if (locked && !databaseWithLock.equals(requestingDatabase)) {
            success = false;
        }
        else {
            locked = true;
            lockCreationTime = System.currentTimeMillis();
            success = true;
            databaseWithLock = requestingDatabase;
        }

        final LockRequestResponse response = new LockRequestResponse(updateCount, success);

        stopWrite();

        if (success) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "LOCATOR LOCKED by database instance '" + requestingDatabase + "'");
        }
        else {
            ErrorHandling.errorNoEvent("Database instance '" + requestingDatabase + "' FAILED TO LOCK the locator server. The lock is held by " + databaseWithLock + ".");
        }
        return response;
    }

    /**
     * Release a lock file. Indicates that a System Table has been created successfully.
     * 
     * @param requestingDatabase
     *            The database which is requesting the lock.
     * @return 0 if the commit failed; 1 if it succeeded.
     */
    public int releaseLockOnFile(final String requestingDatabase) {

        startWrite();

        int result = 0;

        if (!locked || !requestingDatabase.equals(databaseWithLock)) {
            ErrorHandling.errorNoEvent("Tried to release lock, but lock wasn't held by this database.");
            result = 0;
        }
        else {
            updateCount++;
            locked = false;
            lockCreationTime = 0l;
            databaseWithLock = null;
            result = 1;
        }

        stopWrite();

        if (result == 1) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Database instance at '" + requestingDatabase + "' has committed its creation of the system table.");
        }
        else {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Database instance at '" + requestingDatabase + "' has failed to commit its creation of the system table.");
        }

        return result;
    }

    /*
     * ##################################### Reader-writer methods. From: http://beg.projects.cis.ksu.edu/examples/small/readerswriters/
     * #####################################
     */

    private boolean writeCondition() {

        return activeReaders == 0 && !writerPresent;
    }

    private boolean readCondition() {

        return !writerPresent;
    }

    private synchronized void startRead() {

        while (!readCondition()) {
            try {
                wait();
            }
            catch (final InterruptedException ex) {
            }
        }
        ++activeReaders;
    }

    private synchronized void stopRead() {

        --activeReaders;
        notifyAll();
    }

    private synchronized void startWrite() {

        while (!writeCondition()) {
            try {
                wait();
            }
            catch (final InterruptedException ex) {
            }
        }
        writerPresent = true;
    }

    private synchronized void stopWrite() {

        writerPresent = false;
        notifyAll();
    }

    /**
     * Create a new locator file. This is used by various test classes to overwrite old locator files.
     */
    public void createNewLocatorFile() {

        startWrite();

        boolean successful = locatorFile.delete();

        if (!successful) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to delete locator file. It may not have existed.");
        }
        try {
            successful = locatorFile.createNewFile();
            if (!successful) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to create locator file. It may already exist.");
            }
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        stopWrite();
    }

    @Override
    public String toString() {

        return locatorFile.getAbsolutePath();
    }

    public void delete() {

        final boolean successful = locatorFile.delete();

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Deleted Locator server state (successful: " + successful + "). This should only be done to destroy tests.");
    }

}
