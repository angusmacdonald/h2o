package org.h2.command.dml;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashSet;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.manager.TableManager;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MigrationException;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TableInfo;
import org.h2.message.Message;
import org.h2.schema.Schema;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RecreateTableManager extends org.h2.command.ddl.SchemaCommand {

	private String tableName;


	/**
	 * @param session
	 * @param schema
	 */
	public RecreateTableManager(Session session, Schema schema, String tableName) {
		super(session, schema);


		this.tableName = tableName;
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

		Database db = this.session.getDatabase();
		ISystemTableReference systemTableReference = db.getSystemTableReference();
		
		/*
		 * TODO perform a check to see that it isn't already active. 
		 */
		
		String schemaName = "";
		if (getSchema() != null){
			schemaName = getSchema().getName();
		} else {
			schemaName = "PUBLIC";
		}

		TableInfo ti = new TableInfo(tableName, schemaName, db.getURL());
		TableManagerRemote tm = null;

		//TableManager dm = TableManager.createTableManagerFromPersistentStore(ti.getSchemaName(), ti.getSchemaName());
		try {
			tm = new TableManager(ti, db);
			tm.recreateReplicaManagerState();
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
			tm = (TableManagerRemote) UnicastRemoteObject.exportObject(tm, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		db.getChordInterface().bind(ti.getFullTableName(), tm);


		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ti + " recreated on " + db.getURL() + ".");

		systemTableReference.addNewTableManagerReference(ti, tm);

		return 1;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#update(java.lang.String)
	 */
	@Override
	public int update(String transactionName) throws SQLException,
	RemoteException {
		return update();
	}


}
