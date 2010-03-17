package org.h2.h2o.util;

import java.io.Serializable;


/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableInfo implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2023146600034394467L;
	
	private String schemaName;
	private String tableName;
	private long modificationID;
	private int tableSet;
	private String tableType;
	private DatabaseURL dbLocation;

	public TableInfo(String tableName, String schemaName, long modificationID, int tableSet, String tableType,
			DatabaseURL dbLocation){
		
		this(tableName, schemaName);
		
		this.modificationID = modificationID;
		this.tableSet = tableSet;
		this.tableType = tableType;
		this.dbLocation = dbLocation;
	}

	/**
	 * @param tableName
	 * @param schemaName
	 */
	public TableInfo(String tableName, String schemaName) {
		this.tableName = tableName;
		this.schemaName = schemaName;
	}

	/**
	 * @param tableName2
	 */
	public TableInfo(String fqTableName) {
		String[] name = fqTableName.split("\\.");
		
		assert name.length == 2;
		
		this.tableName = name[1];
		this.schemaName = name[0];
	}


	/**
	 * @param tableName2
	 * @param schemaName2
	 * @param databaseURL
	 */
	public TableInfo(String tableName, String schemaName, DatabaseURL databaseURL) {
		this(tableName, schemaName);
		
		this.dbLocation = databaseURL;
	}

	/**
	 * @return the schemaName
	 */
	public String getSchemaName() {
		return schemaName;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the modificationID
	 */
	public long getModificationID() {
		return modificationID;
	}

	/**
	 * @return the tableSet
	 */
	public int getTableSet() {
		return tableSet;
	}

	/**
	 * @return the tableType
	 */
	public String getTableType() {
		return tableType;
	}

	/**
	 * @return the dbLocation
	 */
	public DatabaseURL getDbLocation() {
		return dbLocation;
	}

	/**
	 * The fully qualified name of the database table.
	 * 
	 * <p>e.g. PUBLIC.TEST, where the table name is simply TEST and the
	 * table is in the PUBLIC schema.
	 * @return
	 */
	public String getFullTableName() {
		return schemaName + "." + tableName;
	}

	/**
	 * @return
	 */
	public TableInfo getGenericTableInfo() {
		return new TableInfo(tableName, schemaName);
	}



	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dbLocation == null) ? 0 : dbLocation.hashCode());
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableInfo other = (TableInfo) obj;
		if (dbLocation == null) {
			if (other.dbLocation != null)
				return false;
		} else if (!dbLocation.equals(other.dbLocation))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TableInfo [" + schemaName + "."
				+ tableName + "]";
	}
	
	
}