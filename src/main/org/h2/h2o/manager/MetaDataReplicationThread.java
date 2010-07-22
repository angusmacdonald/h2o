package org.h2.h2o.manager;

import org.h2.engine.Database;
import org.h2.h2o.comms.MetaDataReplicaManager;

public class MetaDataReplicationThread extends Thread {
	private MetaDataReplicaManager metaDataReplicaManager;
	private ISystemTableReference systemTableReference;
	
	private boolean running = true;
	private int threadSleepTime;
	private Database database;
	
	public MetaDataReplicationThread(MetaDataReplicaManager metaDataReplicaManager, ISystemTableReference systemTableReference, Database database, int threadSleepTime) {
		this.metaDataReplicaManager = metaDataReplicaManager;
		this.systemTableReference = systemTableReference;
		this.database = database;
		this.threadSleepTime = threadSleepTime;
	}
	
	public void run(){
		
		while (isRunning()){
			/*
			 * Sleep.
			 */
			try {
				Thread.sleep(threadSleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (!database.isRunning()) return;
			/*
			 * Check that there are a sufficient number of replicas of Table Manager state.
			 */
			metaDataReplicaManager.replicateIfPossible(systemTableReference);
		}
	}
	
	
	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running ;
	}


	/**
	 * @param running the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

}
