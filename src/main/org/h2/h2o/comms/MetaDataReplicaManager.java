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
package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.TableManager;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class MetaDataReplicaManager {
	/**
	 * Manages the location of Table Manager replicas.
	 */
	private ReplicaManager tableManagerReplicas;

	/**
	 * Managers the location of System Table replicas.
	 */
	private ReplicaManager systemTableReplicas;


	/**
	 * Whether the System is replicating meta-data at all.
	 */
	private boolean metaDataReplicationEnabled;

	/**
	 * The number of replicas required of System Table meta-data.
	 */
	private int systemTableReplicationFactor;

	/**
	 * The number of replicas required of Table Manager meta-data.
	 */
	private int tableManagerReplicationFactor;

	private Parser parser;

	/**
	 * Location of the local database instance.
	 */
	private DatabaseInstanceWrapper localDatabase;

	private Database db;

	public MetaDataReplicaManager(boolean metaDataReplicationEnabled, int systemTableReplicationFactor, int tableManagerReplicationFactor, DatabaseInstanceWrapper localDatabase, Database db){
		/*
		 * Replica Locations
		 */
		this.tableManagerReplicas = new ReplicaManager();
		this.systemTableReplicas = new ReplicaManager();

		this.tableManagerReplicas.add(localDatabase);
		this.systemTableReplicas.add(localDatabase);

		/*
		 * Configuration options.
		 */
		this.metaDataReplicationEnabled = metaDataReplicationEnabled;
		this.systemTableReplicationFactor = systemTableReplicationFactor;
		this.tableManagerReplicationFactor = tableManagerReplicationFactor;

		/*
		 * Local Machine Details.
		 */
		this.db = db;
		this.localDatabase = localDatabase;
		this.parser = new Parser(db.getSystemSession(), true);
	}

	public boolean addReplicaLocation(DatabaseInstanceWrapper newReplicaLocation, boolean isSystemTable, String query, String deleteOldEntries) throws RemoteException{
		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable)? systemTableReplicationFactor: tableManagerReplicationFactor;


		if (metaDataReplicationEnabled){
			if (replicaManager.size() < managerStateReplicationFactor){ //+1 because the local copy counts as a replica.

				//now replica state here.
				try {
					/*
					 * Remove existing entries for this System Table / Table Manager.
					 */
					newReplicaLocation.getDatabaseInstance().executeUpdate(deleteOldEntries, true);


					/*
					 * Create new replica if needed, then replicate state.
					 */
					newReplicaLocation.getDatabaseInstance().executeUpdate(query, true);

					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O Schema Tables replicated on new successor node: " + newReplicaLocation.getDatabaseURL().getDbLocation());

					replicaManager.add(newReplicaLocation);

					return true;
				} catch (SQLException e) {
					e.printStackTrace();
					ErrorHandling.errorNoEvent("Failed to replicate manager/table state onto: " + newReplicaLocation.getDatabaseURL().getDbLocation());
				} catch (Exception e) {
					throw new RemoteException(e.getMessage());
				} 

			}
		}
		return false;
	}

	public int executeUpdate(String query, boolean isSystemTable, TableInfo tableInfo) throws SQLException{

		//Loop through replicas
		//Asynchrously send update.

		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;

		Set<DatabaseInstanceWrapper> replicas = replicaManager.getActiveReplicas();
		Set<DatabaseInstanceWrapper> failed = new HashSet<DatabaseInstanceWrapper>();

		int result = -1;

		for (DatabaseInstanceWrapper replica: replicas){
			if (isLocal(replica)){
				Command sqlQuery = parser.prepareCommand(query);

				try {
					result = sqlQuery.update();

					sqlQuery.close();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else {
				try {
					result = replica.getDatabaseInstance().executeUpdate(query, true);
				} catch (RemoteException e) {
					e.printStackTrace();
					failed.add(replica);

					if (!isSystemTable){
						//Remove table replica information from the system table.
						try {
							this.db.getSystemTable().removeTableManagerStateReplica(tableInfo, replica.getDatabaseURL());
						} catch (RemoteException e1) {
							e1.printStackTrace();
						} catch (MovedException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}

		boolean hasRemoved = replicas.removeAll(failed);

		if (hasRemoved){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Removed one or more replica locations because they couldn't be contacted for the last update.");
		}

		return result;
	}

	private boolean isLocal(DatabaseInstanceWrapper replica) {
		return replica.equals(localDatabase);
	}

	public String[] getReplicaLocations(boolean isSystemTable) {

		if (isSystemTable){
			return systemTableReplicas.getReplicaLocations();
		} else {
			return tableManagerReplicas.getReplicaLocations();
		}
	}

	public void remove(DatabaseInstanceRemote databaseInstance, boolean isSystemTable) {
		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;
		
		replicaManager.remove(databaseInstance);
	}


}
