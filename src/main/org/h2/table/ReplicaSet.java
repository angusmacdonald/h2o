package org.h2.table;

import java.util.HashSet;
import java.util.Set;

import org.h2.schema.SchemaObject;


/**
 * H2O. Contains a set of replicas for a single table instance.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaSet {
	
	/**
	 * The name of the table for which these replicas represent.
	 */
	private String tableName;
	
	/**
	 * The set of all replicas of this table.
	 */
	private Set<Table> replicas;
	
	/**
	 * The primary copy of this table.
	 */
	private Table primaryCopy;

	/**
	 * Local copy of the data.
	 */
	private Table localCopy;
	
	/**
	 * Create a ReplicaSet for a new table in the system. The first instance of this table is automatically
	 * made the primary.
	 * @param obj
	 */
	public ReplicaSet(SchemaObject obj){
		
		Table table = (Table) obj;
		
		replicas = new HashSet<Table>();
		
		replicas.add(table);
		
		primaryCopy = table;
		localCopy = (table.isLocal())? table: null;
		
		tableName = primaryCopy.getName();
	}
	
	/**
	 * Get the Table instance associated with the primary copy of this table.
	 * @return
	 */
	public Table getPrimaryCopy(){
		return primaryCopy;
	}
	
	/**
	 * Get a reference to a local copy of the data, if one exists. If not, null is returned.
	 * @return
	 */
	public Table getLocalCopy(){
		return localCopy;
	}
	
	public Set<Table> getAllCopies(){
		return replicas;
	}
	
	/**
	 * Add a new replica for the table this replica set represents.
	 * @param table	The replica to be added.
	 */
	public void addNewReplica(SchemaObject obj){
		Table table = (Table) obj;
		
		localCopy = (table.isLocal())? table: null;
		
		replicas.add(table);
		
		if (primaryCopy == null){
			primaryCopy = table;
		}
	}
	
	/**
	 * The name of the table which these replicas represent.
	 * @return Table name.
	 */
	public String getTableName(){
		return tableName;
	}
	
	public String toString(){
		return getTableName();
	}

	/**
	 * Deconstruct this object.
	 */
	public void removeAllCopies() {
		replicas = null;
		primaryCopy = null;
		localCopy = null;
	}

	/**
	 * Return a single copy of the replica. Try to get a local copy first, but if none exists get another one.
	 * @return
	 */
	public Table getACopy() {
		return (getLocalCopy() == null)? getPrimaryCopy(): getLocalCopy();
	}

	/**
	 * Remove a single copy of this data. If no more copies exist this method will return false; otherwise, true.
	 * @param table	Table to be removed.
	 * @return	True, if other tables exist.
	 */
	public boolean removeCopy(Table table) {
		replicas.remove(table);
		
		if (table == localCopy)
			localCopy = null;
		if (table == primaryCopy)
			primaryCopy = null;
		
		return (replicas.size() == 0)? false: true;
	}

	/** 
	 * Add dependancies to each copy.
	 * @param set
	 */
	public void addDependencies(HashSet set) {
		for (Table table: replicas){
			table.addDependencies(set);
		}
		
	}

	/**
	 * Get the SQL required to create the table and everything it relies on.
	 * @return
	 */
	public String getSQL() {
		return (getLocalCopy() == null)? getPrimaryCopy().getSQL(): getLocalCopy().getSQL();
	}

	/**
	 * The SQL required to create the table
	 * @return
	 */
	public String getCreateSQL() {
		return (getLocalCopy() == null)? getPrimaryCopy().getCreateSQL(): getLocalCopy().getCreateSQL();
	}
}
