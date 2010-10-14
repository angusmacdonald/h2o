/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.util.NetUtils;

/**
 * Example query, connecting to a database that has already been started using either {@link CustomH2OExample} or
 * {@link StandaloneH2OExample}
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class QueryExample {
	
	public static void main(String[] args) {
		String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":9998/db_data//MyFirstDatabase9998";
		
		/*
		 * Default system password. You'd probably want to change this.
		 */
		String userName = "sa";
		String password = "";
		
		try {
			/*
			 * Create connection to the H2O database instance.
			 */
			Connection conn = DriverManager.getConnection(jdbcURL, userName, password);
			
			/*
			 * Create a basic table on the H2O instance and add some even more basic data.
			 */
			Statement stat = conn.createStatement();
			stat.executeUpdate("CREATE TABLE TEST (ID INT);");
			stat.executeUpdate("INSERT INTO TEST VALUES(7);");
			
			/*
			 * Query the database to check that the data was added successfully.
			 */
			ResultSet rs = stat.executeQuery("SELECT * FROM TEST;");
			
			if ( rs.next() ) {
				int result = rs.getInt(1);
				System.out.println("A result was successfully obtained from the database: " + result);
				
				if ( result == 7 ) {
					System.out.println("This was the expected result. Success!");
				} else {
					System.err.println("This was not expected. The operation failed.");
				}
			} else {
				System.err.println("The database didn't return any entries. This wasn't expected.");
			}
		} catch ( SQLException e ) {
			e.printStackTrace();
		}
	}
	
}
