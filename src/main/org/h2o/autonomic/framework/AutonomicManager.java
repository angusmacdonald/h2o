/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.autonomic.framework;

import java.io.Serializable;
import java.util.Observer;

/**
 * <p>
 * An autonomic manager is responsible for some aspect of the database system's operation, such as replication factor.
 * 
 * <p>
 * The manager must register an interest in monitoring events (see {@link AutonomicMonitor}) relevant to its decision making. For example, a
 * manager responsible for controlling replication factor must register for all events that may require a change in the current replication
 * factor.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface AutonomicManager extends Serializable, Observer {

    /**
     * Sends monitoring data to the manager so that the knowledge-base can be updated and analysed. This method is called by classes
     * implementing {@link AutonomicMonitor} where the manager has subscribed for updates from the given class.
     * 
     * @param monitoringData
     *            Data which provides
     */
    public void recieveData(MonitoringData monitoringData);
}
