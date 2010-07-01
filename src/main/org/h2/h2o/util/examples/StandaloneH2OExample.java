package org.h2.h2o.util.examples;


import org.h2.h2o.deployment.H2O;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Creates a standalone H2O instance.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class StandaloneH2OExample {

	public static void main(String[] args) {
		Diagnostic.setLevel(DiagnosticLevel.FINAL);
		
		String databaseName = "MyFirstDatabase";//the name of the database domain.
		int tcpPort = 9999;						//the port on which the databases TCP JDBC server will run.	
		String rootFolder = "db_data"; 			//where the database will be created (where persisted state is stored).

		H2O db = new H2O(databaseName, tcpPort, rootFolder);
		db.startDatabase();
	}

}
