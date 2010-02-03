package org.h2.command.dml;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.command.Prepared;
import org.h2.command.ddl.SchemaCommand;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.table.TableLinkConnection;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class GetRmiPort extends SchemaCommand {

	private String databaseLocation;
	private TableLinkConnection conn;

	/**
	 * @param session
	 * @param internalQuery
	 * @param databaseLocation 
	 */
	public GetRmiPort(Session session, Schema schema, String databaseLocation) {
		super(session, schema);
		
		this.databaseLocation = databaseLocation;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#isTransactional()
	 */
	@Override
	public boolean isTransactional() {
		return false;
	}

	
	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#update()
	 */
	@Override
	public int update() throws SQLException, RemoteException {
		if (databaseLocation == null){
			/*
			 * Return the RMI port on which this database is running.
			 */
			
			return super.session.getDatabase().getDatabaseURL().getRMIPort();
		} else {
			/*
			 * Send this query remotely to get the RMI port.
			 */
			return pushCommand(databaseLocation, "GET RMI PORT");
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#update(java.lang.String)
	 */
	@Override
	public int update(String transactionName) throws SQLException,
			RemoteException {
		return update();
	}

	/**
	 * Push a command to a remote machine where it will be properly executed.
	 * @param createReplica true, if the command being pushed is a create replica command. This results in any subsequent tables
	 * involved in the command also being pushed.
	 * @return The result of the update.
	 * @throws SQLException 
	 * @throws RemoteException 
	 */
	private int pushCommand(String remoteDBLocation, String query) throws SQLException, RemoteException {
		Database db = session.getDatabase();

		conn = db.getLinkConnection("org.h2.Driver", remoteDBLocation, PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

		int result = -1;

		synchronized (conn) {
			try {
				Statement stat = conn.getConnection().createStatement();
				
				stat.execute(query);
				result = stat.getUpdateCount();
				
			} catch (SQLException e) {
				conn.close();
				conn = null;
				e.printStackTrace();
				throw e;
			}
		}

		return result;
	}

}
