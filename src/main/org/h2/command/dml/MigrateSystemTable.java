package org.h2.command.dml;

import java.rmi.RemoteException;
import java.sql.SQLException;
import org.h2.engine.Session;
import org.h2.schema.Schema;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrateSystemTable extends org.h2.command.ddl.SchemaCommand {

	/**
	 * @param session
	 * @param schema
	 */
	public MigrateSystemTable(Session session, Schema schema) {
		super(session, schema);
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
		this.session.getDatabase().getSystemTableReference().migrateSystemTableToLocalInstance();

		return 0;
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
