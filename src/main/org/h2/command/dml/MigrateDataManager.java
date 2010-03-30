package org.h2.command.dml;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.manager.DataManager;
import org.h2.h2o.manager.MigrationException;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.h2o.manager.SchemaManager;
import org.h2.h2o.manager.SchemaManagerRemote;
import org.h2.h2o.util.DatabaseURL;
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
public class MigrateDataManager extends org.h2.command.ddl.SchemaCommand {

	private String tableName;

	/**
	 * @param session
	 * @param schema
	 */
	public MigrateDataManager(Session session, Schema schema, String tableName) {
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
		try {
			Database db = this.session.getDatabase();
			SchemaManagerRemote sm = db.getSchemaManagerReference().getSchemaManager();
			String schemaName = "";
			if (getSchema() != null){
				schemaName = getSchema().getName();
			} else {
				schemaName = "PUBLIC";
			}
			DataManagerRemote dmr = sm.lookup(new TableInfo(tableName, schemaName));

			QueryProxy qp = dmr.getQueryProxy(LockType.WRITE, this.session.getDatabase().getLocalDatabaseInstance());

			if (!qp.getLockGranted().equals(LockType.NONE)){
				migrateDataManagerToLocalInstance(dmr, schemaName, db);
			} else {
				throw Message.getSQLException(ErrorCode.LOCK_TIMEOUT_1, getSchema().getName() + tableName);
			}
		} catch (MovedException e) {
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, getSchema().getName() + tableName);
		} catch (Exception e) {
			e.printStackTrace();
				throw new SQLException("Update failed somehow.");
			}
		

		return 0;
	}

	public void migrateDataManagerToLocalInstance(DataManagerRemote oldDataManager, String schemaName, Database db){
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate data manager.");

		/*
		 * Create a new schema manager instance locally.
		 */
		DataManagerRemote newDataManager = null;
		try {
			newDataManager = new DataManager(tableName, schemaName, 0l, oldDataManager.getTableSet(), db);
		} catch (Exception e) {
			ErrorHandling.hardExceptionError(e, "Failed to create new data manager.");
		}

		/*
		 * Stop the old, remote, manager from accepting any more requests.
		 */
		try {
			oldDataManager.prepareForMigration(db.getDatabaseURL().getURLwithRMIPort());
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "This data manager is already being migrated to another instance.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This data manager has already been migrated to another instance.");
		}

		/*
		 * Build the schema manager's state from that of the existing manager.
		 */
		try {
			newDataManager.buildDataManagerState(oldDataManager);
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to migrate data manager to new machine.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This shouldn't be possible here. The data manager has moved, but this instance should have had exclusive rights to it.");
		}

		/*
		 * Shut down the old, remote, schema manager. Redirect requests to new manager.
		 */
		try {
			oldDataManager.completeMigration();
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to complete migration.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This shouldn't be possible here. The data manager has moved, but this instance should have had exclusive rights to it.");
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
		}
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Data Manager officially migrated.");

		/*
		 * Confirm the new schema managers location by updating all local state.
		 */
		oldDataManager = newDataManager;


		try {
			DataManagerRemote stub = (DataManagerRemote) UnicastRemoteObject.exportObject(newDataManager, 0);
			db.getSchemaManagerReference().getSchemaManager().changeDataManagerLocation(stub, new TableInfo(tableName, schemaName));
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Data manager migration failed.");
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


}
