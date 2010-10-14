/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util;

import org.h2o.db.id.DatabaseURL;

/**
 * Utility class which generates unique names for transactions.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TransactionNameGenerator {
	
	private long lastNumber = 0;
	
	/**
	 * Used when no database name is specified to avoid unnecessarily contacting local instance of this class.
	 */
	private static long lastStaticNumber = 0;
	
	private String prefix;
	
	public TransactionNameGenerator(DatabaseURL url) {
		this.prefix = generatePrefix(url);
	}
	
	public TransactionNameGenerator(DatabaseURL url, long startNumber) {
		this(url);
		this.lastNumber = startNumber;
	}
	
	private String generatePrefix(DatabaseURL url) {
		
		String part = "";
		
		if ( url == null ) {
			part = "UnknownDB";
		} else {
			
			String hostname = url.getHostname();
			
			if ( hostname == null ) {
				hostname = "local";
			}
			
			part = ( url.isTcp() ? hostname.replace(".", "") + url.getPort() : "" ) + url.sanitizedLocation();
			
		}
		
		return part;
	}
	
	/**
	 * Generate a unique name for a new transaction based on the identity of the requesting database.
	 * 
	 * @param requestingDatabase
	 *            Proxy representing the database making the request.
	 * @return
	 */
	public String generateName() {
		return generateFullTransactionName(prefix);
	}
	
	private String generateFullTransactionName(String part) {
		String transactionName = "TRANSACTION_";
		
		transactionName += part;
		
		if ( lastNumber == Long.MAX_VALUE ) {
			lastNumber = -1;
		}
		
		return ( transactionName + "_" + lastNumber++ ).toUpperCase();
	}
	
	/**
	 * Generate a unique name for a new local transaction. Transactions spanning multiple databases should used the other method.
	 * 
	 * @param requestingDatabase
	 * @param tableName
	 *            Name of a table involved in the transaction.
	 * @return
	 */
	public static String generateName(String part) {
		String transactionName = "TRANSACTION_";
		
		transactionName += part;
		
		if ( lastStaticNumber == Long.MAX_VALUE ) {
			lastStaticNumber = -1;
		}
		
		return ( transactionName + "_" + lastStaticNumber++ ).toUpperCase();
	}
	
}
