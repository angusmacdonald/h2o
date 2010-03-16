package org.h2.command.dml;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.schema.Schema;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrateSchemaManager extends org.h2.command.ddl.SchemaCommand {

	/**
	 * @param session
	 * @param schema
	 */
	public MigrateSchemaManager(Session session, Schema schema) {
		super(session, schema);
		// TODO Auto-generated constructor stub
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
		this.session.getDatabase().getSchemaManagerReference().migrateSchemaManagerToLocalInstance();
		
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
