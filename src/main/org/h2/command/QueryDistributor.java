package org.h2.command;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.comms.DataManagerRemote;
import org.h2.h2o.comms.DatabaseInstanceRemote;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.TransactionNameGenerator;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryDistributor {
	
	/**
	 * Send the given update to all replicas of the table.
	 * @return
	 * @throws SQLException
	 */
	public static int propagateUpdate(String fullTableName, String sql, Database db) throws SQLException {
		int count;
		//Get the data manager and send this query to each replica.

		DataManagerRemote dm = db.getDataManager(fullTableName);

		QueryProxy qp = null;

		try {
			if (dm == null){
				System.err.println("Data manager proxy was null when requesting table: " + fullTableName);
				throw new SQLException("Data manager not found for table: " + fullTableName);
			} else {
				qp = dm.requestLock(QueryProxy.LockType.WRITE);
			}
		} catch (RemoteException e1) {
			e1.printStackTrace();
			throw new SQLException("Unable to contact data manager.");
		} catch (SQLException e){
			throw new SQLException("Unable to contact data manager.");
		}

		count = qp.sendToAllReplicas(sql, db);

		return count;
	}
}
