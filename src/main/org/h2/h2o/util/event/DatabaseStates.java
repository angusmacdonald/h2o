package org.h2.h2o.util.event;

public enum DatabaseStates { //comments - what information is appropriate
	DATABASE_STARTUP, //database name, chord position					in Database.java								(Implemented)
	DATABASE_SHUTDOWN, //database name									in Database.java								(Implemented)
	DATABASE_FAILURE, //database name									in ChordRemote.java 							(Implemented)
	TABLE_CREATION, //table name										in CreateTable.java								(Implemented)
	TABLE_DELETION, //table name										in DropTable.java								(Implemented)
	TABLE_WRITE,  //table name, size									in TableManager.java
	TABLE_UPDATE, //table name, size									in TableManager.java
	REPLICA_CREATION, //table name, table size							in CreateReplica.java							(Implemented)
	REPLICA_DELETION, //table name										in DropReplica.java								(Implemented)
	TABLE_MANAGER_CREATION, //table name, database name					in TableManager.java							(Implemented)
	TABLE_MANAGER_MIGRATION, //table name, database name				in MigrateTableManager.java						(Implemented)
	TABLE_MANAGER_REPLICA_CREATION,//database name, location of replicasin MetaDataReplicationManager.java				(Implemented)
	SYSTEM_TABLE_CREATION, //database name								in InMemorySystemTable.java						(Implemented)
	SYSTEM_TABLE_MIGRATION, //database name								in SystemTableReference.java					(Implemented)
	SYSTEM_TABLE_REPLICA_CREATION //database name, location of replicas in MetaDataReplicationManager.java				(Implemented)
}
