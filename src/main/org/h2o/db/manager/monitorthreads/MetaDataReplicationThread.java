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
package org.h2o.db.manager.monitorthreads;

import org.h2.engine.Database;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.replication.MetaDataReplicaManager;

public class MetaDataReplicationThread extends Thread {
	private MetaDataReplicaManager metaDataReplicaManager;
	private ISystemTableReference systemTableReference;

	private boolean running = true;
	private int threadSleepTime;
	private Database database;

	public MetaDataReplicationThread(
			MetaDataReplicaManager metaDataReplicaManager,
			ISystemTableReference systemTableReference, Database database,
			int threadSleepTime) {
		this.metaDataReplicaManager = metaDataReplicaManager;
		this.systemTableReference = systemTableReference;
		this.database = database;
		this.threadSleepTime = threadSleepTime;
	}

	public void run() {
		int i = 0;
		while (isRunning()) {
			if (!database.isRunning())
				continue;

			/*
			 * Sleep.
			 */
			try {
				Thread.sleep(threadSleepTime);
			} catch (InterruptedException e) {
			}
			
			/*
			 * Check that there are a sufficient number of replicas of Table
			 * Manager state.
			 */
			metaDataReplicaManager.replicateMetaDataIfPossible(
					systemTableReference, false);

			/*
			 * Check that there are a sufficient number of replicas of System
			 * Table state.
			 */
			metaDataReplicaManager.replicateMetaDataIfPossible(
					systemTableReference, true);

			if (!database.isRunning())
				return;
			i++;
		}
	}

	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running;
	}

	/**
	 * @param running
	 *            the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

}
