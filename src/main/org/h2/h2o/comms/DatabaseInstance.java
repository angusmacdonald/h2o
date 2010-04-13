package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Proxy class exposed via RMI, allowing semi-parsed queries to be sent to remote replicas for execution.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstance implements DatabaseInstanceRemote {

	/**
	 * The parsed JDBC connection string for this database.
	 */
	private DatabaseURL databaseURL;

	/**
	 * Used to parse queries on this machine.
	 */
	private Parser parser;

	/**
	 * Queries executed in this session.
	 */
	private Session session;

	/**
	 * Whether the database instance is alive or in the process of being shut down.
	 */
	private boolean alive = true;

	public DatabaseInstance(DatabaseURL databaseURL, Session session){
		this.databaseURL = databaseURL;

		this.parser = new Parser(session, true);
		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.dm.DatabaseInstanceRemote#executeUpdate(org.h2.command.Prepared)
	 */
	public int prepare(String query, String transactionName) throws RemoteException, SQLException{
		if (query == null){
			ErrorHandling.hardError("Shouldn't happen.");
		}

		Command command = parser.prepareCommand(query);

		/*
		 * If called from here executeUpdate should always be told the query is part of a larger transaction, because it
		 * was remotely initiated and consequently needs to wait for the remote machine to commit.
		 */
		command.executeUpdate(true);

		command.close();

		return prepare(transactionName);
	}

	public int prepare(String transactionName) throws RemoteException, SQLException{

		assert session.getAutoCommit() == false;

		Command command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
		return command.executeUpdate();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#commitQuery(boolean)
	 */
	@Override
	public int commit(boolean commit, String transactionName) throws RemoteException, SQLException {
		Command command = parser.prepareCommand((commit? "commit": "rollback") + " TRANSACTION " + transactionName);
		int result = command.executeUpdate();

		//session.setAutoCommit(true); //TODO auto-commit shouldn't be set false.true for each transaction.
		return result;
	}

	@Override
	@Deprecated
	/**
	 * @Deprecated Because it doesn't currently pass in a transaction name. Do we generate one is this method???
	 */
	public int executeUpdate(QueryProxy queryProxy, String sql) throws RemoteException,
	SQLException {
		/*
		 * TODO eventually this method may do a lot more - e.g. the query may only be run here, and asynchronously run elsewhere. Another
		 * overloaded version of the method may take only the query string and be required to obtain the queryProxy seperately. 
		 */
		return queryProxy.executeUpdate(sql, null, session);
	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#getConnectionString()
	 */
	@Override
	public String getConnectionString() throws RemoteException {
		return databaseURL.getOriginalURL();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#getLocation()
	 */
	@Override
	public DatabaseURL getConnectionURL()  throws RemoteException {
		return databaseURL;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#getSchemaManagerLocation()
	 */
	@Override
	public DatabaseURL getSchemaManagerURL() throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Responding to request for schema manager location at database '" + session.getDatabase().getDatabaseLocation() + "'.");
		return session.getDatabase().getSchemaManagerReference().getSchemaManagerURL();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String sql, boolean schemaManagerCommand)  throws RemoteException, SQLException  {
		Command command = null;
		if (schemaManagerCommand){
			Parser schemaParser = new Parser(session.getDatabase().getH2OSession(), true);
			command = schemaParser.prepareCommand(sql);
		} else {
			command = parser.prepareCommand(sql);
		}

		int result = command.executeUpdate(false);
		command.close();

		return result;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#setSchemaManagerLocation(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	@Override
	public void setSchemaManagerLocation(IChordRemoteReference schemaManagerLocation, DatabaseURL databaseURL) throws RemoteException {
		this.session.getDatabase().getSchemaManagerReference().setSchemaManagerLocation(schemaManagerLocation, databaseURL);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DatabaseInstanceRemote#findDataManagerReference(org.h2.h2o.util.TableInfo)
	 */
	@Override
	public DataManagerRemote findDataManagerReference(TableInfo ti)
	throws RemoteException {
		try {
			return this.session.getDatabase().getSchemaManagerReference().lookup(ti);
		} catch (SQLException e) {
			ErrorHandling.errorNoEvent("Couldn't find data manager at this machine. Data manager needs to be re-instantiated.."); //TODO allow for re-instantiation at this point.
			return null;
		}
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#testAvailability()
	 */
	@Override
	public boolean isAlive() throws RemoteException {
		return alive;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 31 + ((databaseURL.getUrlMinusSM() == null) ? 0
				: databaseURL.getUrlMinusSM().hashCode());
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
		DatabaseInstance other = (DatabaseInstance) obj;
		if (databaseURL.getUrlMinusSM() == null) {
			if (other.databaseURL.getUrlMinusSM() != null)
				return false;
		} else if (!databaseURL.getUrlMinusSM()
				.equals(other.databaseURL.getUrlMinusSM()))
			return false;
		return true;
	}
}
