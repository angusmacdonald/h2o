/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator;

import org.h2o.locator.messages.LockRequestResponse;
import org.h2o.locator.messages.ReplicaLocationsResponse;

/**
 * Static classes which create requests and responses to be sent to/from the locator server.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 * 
 */
public class LocatorProtocol {

    /**
     * Indicates lock request is being made. Used when an instance wants to create a new System Table.
     */
    public static final String LOCK = "LOCK";

    /**
     * Indicates an instance has created a System Table and wishes to commit this action.
     */
    public static final String COMMIT = "COMMIT";

    /**
     * Request to get the list of current System Table replicas.
     */
    public static final String GET = "GET";

    /**
     * Request to update the list of current System Table replicas.
     */
    public static final String SET = "SET";

    private static final String END_DELIMETER = "\nEND\n";

    /**
     * Creates a 'Get Replica Locations' request.
     * 
     * <p>
     * GET REPLICA LOCATIONS
     * <ul>
     * <li>Request: GET REPLICA LOCATIONS</li>
     * <li>Response: (update count) (replica location <delimiter>)*</li>
     * </ul>
     * 
     * @return The string of the request to be sent.
     */
    public static String constructGetRequest() {

        return GET + END_DELIMETER;
    }

    /**
     * Creates a 'Get Replica Locations' response.
     * 
     * <p>
     * GET REPLICA LOCATIONS
     * <ul>
     * <li>Request: GET REPLICA LOCATIONS</li>
     * <li>Response: (update count) (replica location <delimiter>)*</li>
     * </ul>
     * 
     * @return The string of the response to be sent.
     */
    public static String constructGetResponse(final ReplicaLocationsResponse response) {

        final StringBuilder message = new StringBuilder();
        message.append(response.getUpdateCount() + "\n");
        for (final String location : response.getLocations()) {
            message.append(location);
            message.append("\n");
        }

        return message.toString();
    }

    /**
     * Creates a 'Set Replica Locations' request.
     * 
     * <p>
     * SET REPLICA LOCATIONS
     * <ul>
     * <li>Request: SET REPLICA LOCATIONS (replica location <delimiter>)*</li>
     * <li>Response: [successful | failed ]</li>
     * </ul>
     * 
     * @return The string of the request to be sent.
     */
    public static String constructSetRequest(final String[] locations) {

        final String delimeter = "\n";

        final StringBuilder message = new StringBuilder();
        message.append(SET + delimeter);

        for (final String location : locations) {

            message.append(location);
            message.append(delimeter);

        }

        message.append(END_DELIMETER);

        return message.toString();
    }

    /**
     * Creates a 'Lock' request, indicating the instance wants to create a new System Table instance.
     * 
     * <p>
     * LOCK
     * <ul>
     * <li>Request: LOCK</li>
     * <li>Response: [successful | failed ] (update count)</li>
     * </ul>
     * 
     * @return The string of the request to be sent.
     */
    public static String constructLockRequest(final String databaseURL) {

        return LOCK + "\n" + databaseURL + END_DELIMETER;
    }

    /**
     * Creates a 'Lock' response, indicating whether the lock was successfully taken out.
     * 
     * <p>
     * LOCK
     * <ul>
     * <li>Request: LOCK</li>
     * <li>Response: [successful | failed ] (update count)</li>
     * </ul>
     * 
     * The response will be '0' if the lock attempt failed, and a positive number if it succeeded. The positive number is the update count.
     * 
     * @return The string of the response to be sent.
     */
    public static int constructLockResponse(final LockRequestResponse response) {

        return response.isSuccessful() ? response.getUpdateCount() : 0;
    }

    /**
     * Parse the result of the lock request.
     * 
     * @param response
     *            The response recieved from the server.
     * @return The update count recieved from the server. This is '0' if no lock was granted.
     */
    public static int parseLockResponse(final String response) {

        final int result = Integer.parseInt(response);
        return result;
    }

    /**
     * Creates a 'Commit' request, indicating the instance has created a System Table and wants to confirm it.
     * 
     * <p>
     * CONFIRM
     * <ul>
     * <li>Request: COMMIT</li>
     * <li>Response: [successful | failed ]</li>
     * </ul>
     * 
     * @return The string of the request to be sent.
     */
    public static String constructCommitRequest(final String databaseURL) {

        return COMMIT + "\n" + databaseURL + END_DELIMETER;
    }
}
