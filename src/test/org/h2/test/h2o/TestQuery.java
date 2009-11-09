package org.h2.test.h2o;

/**
 * Represents an SQL query and its expected result.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TestQuery {
	private String query;
	private int[] pKey;
	private String[] secondColumn;
	private String tableName;
	
	/**
	 * @param query
	 * @param pKey
	 * @param secondColumn
	 */
	public TestQuery(String query, String tableName, int[] pKey, String[] secondColumn) {
		this.tableName = tableName;
		this.query = query;
		this.pKey = pKey;
		this.secondColumn = secondColumn;
	}
	
	/**
	 * @return the query
	 */
	public String getSQL() {
		return query;
	}
	/**
	 * @return the pKey
	 */
	public int[] getPrimaryKey() {
		return pKey;
	}
	/**
	 * @return the secondColumn
	 */
	public String[] getSecondColumn() {
		return secondColumn;
	}

	/**
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}
	
	
}
