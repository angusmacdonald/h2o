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
package org.h2o.event;

public enum DatabaseStates { // comments - what information is appropriate
	DATABASE_STARTUP, // database name, chord position in Database.java
						// (Implemented)
	DATABASE_SHUTDOWN, // database name in Database.java (Implemented)
	DATABASE_FAILURE, // database name in ChordRemote.java (Implemented)
	TABLE_CREATION, // table name in CreateTable.java (Implemented)
	TABLE_DELETION, // table name in DropTable.java (Implemented)
	TABLE_WRITE, // table name, size in TableManager.java
	TABLE_UPDATE, // table name, size in TableManager.java
	REPLICA_CREATION, // table name, table size in CreateReplica.java
						// (Implemented)
	REPLICA_DELETION, // table name in DropReplica.java (Implemented)
	TABLE_MANAGER_CREATION, // table name, database name in TableManager.java
							// (Implemented)
	TABLE_MANAGER_MIGRATION, // table name, database name in
								// MigrateTableManager.java (Implemented)
	TABLE_MANAGER_SHUTDOWN, // TableManager.java (Implemented).
	TABLE_MANAGER_REPLICA_CREATION, // database name, location of replicasin
									// MetaDataReplicationManager.java
									// (Implemented)
	SYSTEM_TABLE_CREATION, // database name in InMemorySystemTable.java
							// (Implemented)
	SYSTEM_TABLE_MIGRATION, // database name in SystemTableReference.java
							// (Implemented)
	SYSTEM_TABLE_REPLICA_CREATION
	// database name, location of replicas in MetaDataReplicationManager.java
	// (Implemented)

}
