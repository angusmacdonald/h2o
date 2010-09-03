package org.h2.command.h2o;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.TableManager;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.event.DatabaseStates;
import org.h2o.event.client.H2OEvent;
import org.h2o.event.client.H2OEventBus;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RecreateTableManager extends org.h2.command.ddl.SchemaCommand {

	private String tableName;
	private String oldPrimaryLocation;

	/**
	 * @param session
	 * @param schema
	 */
	public RecreateTableManager(Session session, Schema schema,
			String tableName, String oldPrimaryLocation) {
		super(session, schema);

		if (oldPrimaryLocation.startsWith("'"))
			oldPrimaryLocation = oldPrimaryLocation.substring(1);
		if (oldPrimaryLocation.endsWith("'"))
			oldPrimaryLocation = oldPrimaryLocation.substring(0,
					oldPrimaryLocation.length() - 1);

		this.oldPrimaryLocation = oldPrimaryLocation;

		this.tableName = tableName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#isTransactional()
	 */
	@Override
	public boolean isTransactional() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#update()
	 */
	@Override
	public int update() throws SQLException, RemoteException {

		Database db = this.session.getDatabase();
		ISystemTableReference systemTableReference = db
				.getSystemTableReference();

		/*
		 * TODO perform a check to see that it isn't already active.
		 */

		String schemaName = "";
		if (getSchema() != null) {
			schemaName = getSchema().getName();
		} else {
			schemaName = "PUBLIC";
		}

		TableInfo ti = new TableInfo(tableName, schemaName, db.getURL());
		TableManager tm = null;

		try {
			tm = new TableManager(ti, db);
			tm.recreateReplicaManagerState(oldPrimaryLocation);
			tm.persistToCompleteStartup(ti);
			tm.persistReplicaInformation();
			H2OEventBus.publish(new H2OEvent(db.getURL(), DatabaseStates.TABLE_MANAGER_CREATION, ti.getFullTableName()));
			
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		/*
		 * Make Table Manager serializable first.
		 */
		try {
			TableManagerRemote tmr = (TableManagerRemote) UnicastRemoteObject
					.exportObject(tm, 0);
			db.getChordInterface().bind(ti.getFullTableName(), tmr);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ti + " recreated on "
				+ db.getURL() + ".");

		systemTableReference.addNewTableManagerReference(ti, tm);

		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#update(java.lang.String)
	 */
	@Override
	public int update(String transactionName) throws SQLException,
			RemoteException {
		return update();
	}

}
