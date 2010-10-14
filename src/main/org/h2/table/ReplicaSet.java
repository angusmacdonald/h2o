package org.h2.table;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.SchemaObject;
import org.h2.util.ObjectArray;

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
	 * Create a ReplicaSet for a new table in the system. The first instance of this table is automatically made the primary.
	 * 
	 * @param obj
	 */
	public ReplicaSet(SchemaObject obj) {
		
		Table table = (Table) obj;
		
		replicas = new HashSet<Table>();
		
		replicas.add(table);
		
		primaryCopy = table;
		localCopy = ( table.isLocal() ) ? table : null;
		
		tableName = primaryCopy.getName();
	}
	
	/**
	 * Get the Table instance associated with the primary copy of this table.
	 * 
	 * @return
	 */
	public Table getPrimaryCopy() {
		return primaryCopy;
	}
	
	/**
	 * Get a reference to a local copy of the data, if one exists. If not, null is returned.
	 * 
	 * @return
	 */
	public Table getLocalCopy() {
		return localCopy;
	}
	
	public Set<Table> getAllCopies() {
		return replicas;
	}
	
	/**
	 * Add a new replica for the table this replica set represents.
	 * 
	 * @param table
	 *            The replica to be added.
	 */
	public void addNewReplica(SchemaObject obj) {
		Table table = (Table) obj;
		
		localCopy = ( table.isLocal() ) ? table : null;
		
		replicas.add(table);
		
		if ( primaryCopy == null ) {
			primaryCopy = table;
		}
	}
	
	/**
	 * The name of the table which these replicas represent.
	 * 
	 * @return Table name.
	 */
	public String getTableName() {
		return tableName;
	}
	
	@Override
	public String toString() {
		return getTableName();
	}
	
	/**
	 * Deconstruct this object.
	 */
	public void removeAllCopies() {
		replicas = new HashSet<Table>();
		primaryCopy = null;
		localCopy = null;
	}
	
	/**
	 * Return a single copy of the replica. Try to get a local copy first, but if none exists get another one.
	 * 
	 * @return
	 */
	public Table getACopy() {
		return ( getLocalCopy() == null ) ? getPrimaryCopy() : getLocalCopy();
	}
	
	/**
	 * Remove a single copy of this data. If no more copies exist this method will return false; otherwise, true.
	 * 
	 * @param table
	 *            Table to be removed.
	 * @return True, if other tables exist.
	 */
	public boolean removeCopy(Table table) {
		replicas.remove(table);
		
		if ( table == localCopy )
			localCopy = null;
		if ( table == primaryCopy )
			primaryCopy = null;
		
		return ( replicas.size() == 0 ) ? false : true;
	}
	
	/**
	 * Add dependancies to each copy.
	 * 
	 * @param set
	 */
	public void addDependencies(Set set) {
		if ( replicas != null && replicas.size() > 0 ) {
			for ( Table table : replicas ) {
				table.addDependencies(set);
			}
		}
	}
	
	/**
	 * Get the SQL required to create the table and everything it relies on.
	 * 
	 * @return
	 */
	public String getSQL() {
		if ( getLocalCopy() != null ) {
			return getLocalCopy().getSQL();
		} else if ( getPrimaryCopy() != null ) {
			return getPrimaryCopy().getSQL();
		}
		return null;
	}
	
	/**
	 * The SQL required to create the table
	 * 
	 * @return
	 */
	public String getCreateSQL() {
		if ( getLocalCopy() != null ) {
			return getLocalCopy().getCreateSQL();
		} else if ( getPrimaryCopy() != null ) {
			return getPrimaryCopy().getCreateSQL();
		}
		return null;
	}
	
	/**
	 * @throws SQLException
	 * 
	 */
	public void checkRename() throws SQLException {
		if ( replicas != null && replicas.size() > 0 ) {
			for ( Table table : replicas ) {
				table.checkRename();
			}
		}
	}
	
	/**
	 * @param newName
	 * @throws SQLException
	 */
	public void rename(String newName) throws SQLException {
		if ( replicas != null && replicas.size() > 0 ) {
			for ( Table table : replicas ) {
				table.rename(newName);
			}
		}
	}
	
	public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType, int headPos,
			String comment) throws SQLException {
		
		Index index = null;
		for ( Table table : replicas ) {
			Index tempIndex = table.addIndex(session, indexName, indexId, cols, indexType, headPos, comment);
			
			if ( table == localCopy ) {
				index = tempIndex;
			}
		}
		return index;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#addRow(org.h2.engine.Session, org.h2.result.Row)
	 */

	public void addRow(Session session, Row row) throws SQLException {
		for ( Table table : replicas ) {
			table.addRow(session, row);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#canDrop()
	 */

	public boolean canDrop() {
		if ( localCopy == primaryCopy ) {
			return primaryCopy.canDrop();
		} else {
			return false;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#canGetRowCount()
	 */

	public boolean canGetRowCount() {
		if ( localCopy != null ) {
			return localCopy.canGetRowCount();
		} else if ( primaryCopy != null ) {
			return primaryCopy.canGetRowCount();
		} else {
			return ( (Table) replicas.toArray()[0] ).canGetRowCount();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#checkSupportAlter()
	 */

	public void checkSupportAlter() throws SQLException {
		for ( Table table : replicas ) {
			table.checkSupportAlter();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#close(org.h2.engine.Session)
	 */

	public void close(Session session) throws SQLException {
		for ( Table table : replicas ) {
			table.close(session);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getIndexes()
	 */

	public ObjectArray getIndexes() {
		Message.throwInternalError("Can't be called yet.");
		
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getMaxDataModificationId()
	 */

	public long getMaxDataModificationId() {
		
		long max = 0;
		
		for ( Table table : replicas ) {
			if ( max < table.getMaxDataModificationId() ) {
				max = table.getMaxDataModificationId();
			}
		}
		return max;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getRowCount(org.h2.engine.Session)
	 */

	public long getRowCount(Session session) throws SQLException {
		if ( localCopy != null ) {
			return localCopy.getRowCount(session);
		} else if ( primaryCopy != null ) {
			return primaryCopy.getRowCount(session);
		} else {
			return ( (Table) replicas.toArray()[0] ).getRowCount(session);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getRowCountApproximation()
	 */

	public long getRowCountApproximation() {
		if ( localCopy != null ) {
			return localCopy.getRowCountApproximation();
		} else if ( primaryCopy != null ) {
			return primaryCopy.getRowCountApproximation();
		} else {
			return ( (Table) replicas.toArray()[0] ).getRowCountApproximation();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getScanIndex(org.h2.engine.Session)
	 */

	public Index getScanIndex(Session session) throws SQLException {
		Message.throwInternalError("Can't be called yet.");
		
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getTableType()
	 */

	public String getTableType() {
		Message.throwInternalError("Shouldn't be called.");
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#getUniqueIndex()
	 */

	public Index getUniqueIndex() {
		Message.throwInternalError("Shouldn't be called.");
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#isLocal()
	 */

	public boolean isLocal() {
		Message.throwInternalError("Shouldn't be called.");
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#isLockedExclusively()
	 */

	public boolean isLockedExclusively() {
		Message.throwInternalError("Shouldn't be called.");
		return false;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#lock(org.h2.engine.Session, boolean, boolean)
	 */

	public void lock(Session session, boolean exclusive, boolean force) throws SQLException {
		Message.throwInternalError("Shouldn't be called.");
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#removeRow(org.h2.engine.Session, org.h2.result.Row)
	 */

	public void removeRow(Session session, Row row) throws SQLException {
		for ( Table table : replicas ) {
			table.removeRow(session, row);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#truncate(org.h2.engine.Session)
	 */

	public void truncate(Session session) throws SQLException {
		Message.throwInternalError("Can't be called yet.");
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.table.Table#unlock(org.h2.engine.Session)
	 */

	public void unlock(Session s) {
		Message.throwInternalError("Can't be called yet.");
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.engine.DbObjectBase#getDropSQL()
	 */

	public String getDropSQL() {
		if ( localCopy != null ) {
			return localCopy.getDropSQL();
		} else if ( primaryCopy != null ) {
			return primaryCopy.getDropSQL();
		} else {
			return ( (Table) replicas.toArray()[0] ).getDropSQL();
		}
	}
	
	/**
	 * The number of replicas for this table.
	 * 
	 * @return Number of replicas.
	 */
	public int size() {
		return replicas.size();
	}
}
