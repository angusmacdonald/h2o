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
public class MigrateTableManager extends org.h2.command.ddl.SchemaCommand {

	private String tableName;


	/**
	 * @param session
	 * @param schema
	 */
	public MigrateTableManager(Session session, Schema schema, String tableName) {
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
		int result = -1;

		try {
			Database db = this.session.getDatabase();
			ISystemTableReference sm = db.getSystemTableReference();
			String schemaName = "";
			if (getSchema() != null){
				schemaName = getSchema().getName();
			} else {
				schemaName = "PUBLIC";
			}
			TableManagerRemote dmr = sm.lookup(new TableInfo(tableName, schemaName));

			if (dmr == null){
				Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, getSchema().getName() + tableName);
			}
			QueryProxy qp = dmr.getQueryProxy(LockType.WRITE, db.getLocalDatabaseInstanceInWrapper());

			if (!qp.getLockGranted().equals(LockType.NONE)){
				result = migrateTableManagerToLocalInstance(dmr, schemaName, db);

				if (result == -1){
					throw new SQLException("Table Manager migration failed.");
				}
			} else {
				throw Message.getSQLException(ErrorCode.LOCK_TIMEOUT_1, getSchema().getName() + tableName);
			}
		} catch (MovedException e) {
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, getSchema().getName() + tableName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("Failed to migrate table manager for " + getSchema().getName() + tableName + ".");
		}


		return result;
	}

	public int migrateTableManagerToLocalInstance(TableManagerRemote oldTableManager, String schemaName, Database db){
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate Table Manager for [" + schemaName + "." + tableName);

		/*
		 * Create a new System Table instance locally.
		 */
		TableManagerRemote newTableManager = null;

		TableInfo ti = new TableInfo(tableName, schemaName, 0l, 0, "TABLE", db.getDatabaseURL());

		try {
			newTableManager = new TableManager(ti, db);
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Failed to create new Table Manager [" + schemaName + "." + tableName + "].");
		}

		/*
		 * Stop the old, remote, manager from accepting any more requests.
		 */
		try {
			oldTableManager.prepareForMigration(db.getDatabaseURL().getURLwithRMIPort());
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "This Table Manager [" + schemaName + "." + tableName + "] is already being migrated to another instance.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This Table Manager [" + schemaName + "." + tableName + "] has already been migrated to another instance.");
		}

		/*
		 * Build the System Table's state from that of the existing manager.
		 */
		try {
			newTableManager.buildTableManagerState(oldTableManager);
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to migrate Table Manager [" + schemaName + "." + tableName + "] to new machine.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This shouldn't be possible here. The Table Manager [" + schemaName + "." + tableName + "] has moved, but this instance should have had exclusive rights to it.");
		}

		/*
		 * Shut down the old, remote, System Table. Redirect requests to new manager.
		 */
		try {
			oldTableManager.completeMigration();
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to complete migration [" + schemaName + "." + tableName + "].");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This shouldn't be possible here. The Table Manager has moved, but this instance should have had exclusive rights to it.");
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "Migration process timed out [" + schemaName + "." + tableName + "]. It took too long.");
		}
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Table Manager [" + schemaName + "." + tableName + "] officially migrated.");

		/*
		 * Confirm the new System Tables location by updating all local state.
		 */
		oldTableManager = newTableManager;


		try {

			TableManagerRemote stub = (TableManagerRemote) UnicastRemoteObject.exportObject(newTableManager, 0);

			db.getSystemTableReference().getSystemTable().changeTableManagerLocation(stub, ti);
			db.getSystemTableReference().addProxy(ti, stub);
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Table Manager migration failed [" + schemaName + "." + tableName + "].");
		}
		
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
