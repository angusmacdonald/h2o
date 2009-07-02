package org.h2.test.h2o;

import static org.junit.Assert.fail;

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
public class TestB implements Runnable {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException {

		Connection conn2 = null;
		Server server2 = null;

		try {
			
			Class.forName("org.h2.Driver");
			
			server2 = Server.createTcpServer(new String[] { "-tcpPort", "9082", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test1" });
			
			new Thread(server2.start()).start();

			conn2 = DriverManager.getConnection("jdbc:h2:tcp://localhost:9082/db_data/unittests/other/schema_test2", "sa", "sa");

			System.out.println("Finished connecting.");
			
			Statement stat = conn2.createStatement();

			stat.executeQuery("SELECT * FROM H20.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H20.H2O_REPLICA");
			ResultSet rs = stat.executeQuery("SELECT * FROM H20.H2O_CONNECTION");

			
			if (rs.next()){
				System.out.println(rs.getString(1) + ", " + rs.getString(2)  + ", " + rs.getString(3) + ", " + rs.getString(4));
			}
			

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		} finally {
			try {
				
				conn2.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			server2.stop();
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			main(new String[0]);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
