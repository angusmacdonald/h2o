package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.Server;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TestA{

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException {

		Connection conn1 = null;
		Server server1 = null;

		System.out.println("hello");
		// Create file 
		FileWriter fstream = new FileWriter("testing.txt");
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("Hello Java");


		try {
			server1 = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test1"});

			new Thread(server1.start()).start();

			Class.forName("org.h2.Driver");
			conn1 = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test1", "sa", "sa");


			Statement stat = conn1.createStatement();

			stat.executeQuery("SELECT * FROM H20.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H20.H2O_REPLICA");
			ResultSet rs = stat.executeQuery("SELECT * FROM H20.H2O_CONNECTION");


			if (rs.next()){
				System.out.println(rs.getString(1) + ", " + rs.getString(2)  + ", " + rs.getString(3) + ", " + rs.getString(4));
			}


		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		} 

		//Close the output stream
		out.close();
	}

}
