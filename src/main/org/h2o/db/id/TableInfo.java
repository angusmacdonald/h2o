/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.id;

import java.io.Serializable;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableInfo implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -2023146600034394467L;

    private final String tableName;

    private final String schemaName;

    private long modificationID;

    private int tableSet;

    private String tableType;

    private DatabaseID dbLocation;

    public TableInfo(final String tableName, final String schemaName, final long modificationID, final int tableSet, final String tableType, final DatabaseID dbLocation) {

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
    public TableInfo(final String tableName, final String schemaName) {

        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    /**
     * @param tableName2
     */
    public TableInfo(final String fqTableName) {

        final String[] name = fqTableName.split("\\.");

        assert name.length == 2;

        tableName = name[1];
        schemaName = name[0];
    }

    /**
     * @param tableName2
     * @param schemaName2
     * @param databaseID
     */
    public TableInfo(final String tableName, final String schemaName, final DatabaseID databaseID) {

        this(tableName, schemaName);

        dbLocation = databaseID;
    }

    public TableInfo(final TableInfo tableInfo) {

        this(tableInfo.getTableName(), tableInfo.getSchemaName());
    }

    public TableInfo(final TableInfo tableInfo, final DatabaseID url) {

        this(tableInfo.getTableName(), tableInfo.getSchemaName());

        dbLocation = url;
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
    public DatabaseID getDatabaseID() {

        return dbLocation;
    }

    /**
     * The fully qualified name of the database table.
     * 
     * <p>
     * e.g. PUBLIC.TEST, where the table name is simply TEST and the table is in the PUBLIC schema.
     * 
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

    public void setURL(final DatabaseID url) {

        dbLocation = url;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (dbLocation == null ? 0 : dbLocation.hashCode());
        result = prime * result + (schemaName == null ? 0 : schemaName.hashCode());
        result = prime * result + (tableName == null ? 0 : tableName.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final TableInfo other = (TableInfo) obj;
        if (dbLocation == null) {
            if (other.dbLocation != null) { return false; }
        }
        else if (!dbLocation.equals(other.dbLocation)) { return false; }
        if (schemaName == null) {
            if (other.schemaName != null) { return false; }
        }
        else if (!schemaName.equals(other.schemaName)) { return false; }
        if (tableName == null) {
            if (other.tableName != null) { return false; }
        }
        else if (!tableName.equals(other.tableName)) { return false; }
        return true;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "TableInfo [" + schemaName + "." + tableName + "]";
    }

}
