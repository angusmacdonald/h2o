/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.autonomic.decision.requests;

import java.io.Serializable;

public class ActionRequest implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3656189458012849935L;

	/**
	 * The time this action is expected to take in milliseconds.
	 */
	public long expectedTimeToCompletion;

	/**
	 * The amount of disk space needed to complete the action (in KB).
	 */
	public long immediateDiskSpace;

	/*
	 * ####################### PROBABILITY-BASED. #######################
	 * 
	 * cpu + memory + network + disk should add up to one.
	 */
	/**
	 * The importance of CPU capacity in this operation.
	 */
	public double cpu;

	/**
	 * The importance of memory capacity in this operation.
	 */
	public double memory;

	/**
	 * The importance of network capacity in this operation.
	 */
	public double network;
	
	/**
	 * The perceived importance of disk space in the future.
	 * 
	 * For example, a create table operation does not require much disk space
	 * immediately, but if a lot of data is subsequently added to the table it
	 * needs more space.
	 */
	public double disk;

	public ActionRequest(long expectedTimeToCompletion,
			long immediateDiskSpace, double cpu,
			double memory, double network, double disk) {
		this.expectedTimeToCompletion = expectedTimeToCompletion;
		this.immediateDiskSpace = immediateDiskSpace;

		this.cpu = cpu;
		this.memory = memory;
		this.network = network;
		this.disk = disk;
	}
}
