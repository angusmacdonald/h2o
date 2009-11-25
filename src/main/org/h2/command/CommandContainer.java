/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.expression.Parameter;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.result.LocalResult;
import org.h2.test.h2o.H2OTest;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * Represents a single SQL statements.
 * It wraps a prepared statement.
 */
public class CommandContainer extends Command {

	private Prepared prepared;

	private QueryProxyManager proxyManager = null;

	CommandContainer(Parser parser, String sql, Prepared prepared) {
		super(parser, sql);
		prepared.setCommand(this);
		this.prepared = prepared;

		/*
		 * If this command is part of a larger transaction then this query proxy manager will be
		 * over-written later on by a call from the Command list class. 
		 */
		if (!session.getApplicationAutoCommit() && session.getCurrentTransactionLocks() != null){
			//Diagnostic.traceNoEvent(Diagnostic.FULL, "Using an existing proxy manager.");
			this.proxyManager = session.getCurrentTransactionLocks();
		} else {
			//Diagnostic.traceNoEvent(Diagnostic.FULL, "Creating a new proxy manager.");
			this.proxyManager = new QueryProxyManager(parser.getSession().getDatabase(), getSession());
			session.setCurrentTransactionLocks(this.proxyManager);
		}
	}

	public ObjectArray getParameters() {
		return prepared.getParameters();
	}

	public boolean isTransactional() {
		return prepared.isTransactional();
	}

	public boolean isQuery() {
		return prepared.isQuery();
	}

	public int executeUpdate() throws SQLException {
		return executeUpdate(false);
	}

	private void recompileIfRequired() throws SQLException {
		if (prepared.needRecompile()) {
			// TODO test with 'always recompile'
			prepared.setModificationMetaId(0);
			String sql = prepared.getSQL();
			ObjectArray oldParams = prepared.getParameters();
			Parser parser = new Parser(session, false);
			prepared = parser.parseOnly(sql);
			long mod = prepared.getModificationMetaId();
			prepared.setModificationMetaId(0);
			ObjectArray newParams = prepared.getParameters();
			for (int i = 0; i < newParams.size(); i++) {
				Parameter old = (Parameter) oldParams.get(i);
				if (old.isValueSet()) {
					Value v = old.getValue(session);
					Parameter p = (Parameter) newParams.get(i);
					p.setValue(v);
				}
			}
			prepared.prepare();
			prepared.setModificationMetaId(mod);
		}
	}

	public int update() throws SQLException {
		int resultOfUpdate = update(false);

		//		if (!getSession().getAutoCommit()){
		//			ErrorHandling.hardError("Unexpected code path. This shouldn't be called when auto-commit is off.");
		//		}

		return resultOfUpdate;
	}

	public LocalResult query(int maxrows) throws SQLException {
		return query(maxrows, false);
	}


	/* (non-Javadoc)
	 * @see org.h2.command.Command#query(int, boolean)
	 */
	@Override
	protected LocalResult query(int maxrows, boolean partOfMultiQueryTransaction)
	throws SQLException {
		recompileIfRequired();
		// TODO query time: should keep lock time separate from running time
		start();
		prepared.checkParameters();
		LocalResult result = prepared.query(maxrows);
		prepared.trace(startTime, result.getRowCount());
		return result;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#update(boolean)
	 */
	@Override
	protected int update(boolean partOfMultiQueryTransaction) throws SQLException {
		recompileIfRequired();
		start();
		prepared.checkParameters();
		int updateCount;


		boolean singleQuery = !partOfMultiQueryTransaction, transactionCommand = prepared.isTransactionCommand();

		if (!transactionCommand){ //Not a prepare or commit.
			assert(proxyManager != null);

			//Acquire distributed locks. 
			QueryProxy proxy = this.acquireLocks(proxyManager); 

			assert(proxy != null);

			proxyManager.addProxy(proxy);	//checks that a lock is held for table, then adds the proxy.

			if (Diagnostic.getLevel() == Diagnostic.FULL){
				proxyManager.addSQL(prepared.getSQL());
			}

			updateCount = prepared.update(proxyManager.getTransactionName());

			boolean commit = true; //An exception would already have been thrown if it should have been a rollback.


			H2OTest.createTableFailure();


			if (singleQuery && session.getApplicationAutoCommit()){ 
				/*
				 * If it is one of a number of queries in the transaction then we must wait for the entire transaction to finish.
				 */

				proxyManager.commit(commit);
				session.setCurrentTransactionLocks(null);
			} else {
				session.setCurrentTransactionLocks(proxyManager);
			}
		} else {
			/*
			 * This is a transaction command. No need to commit such a query.
			 */
			
			try {
				updateCount = prepared.update();
				session.setCurrentTransactionLocks(null);
			} catch (SQLException e){
				System.err.println("Transaction not found for query: " + prepared.getSQL());
				throw e;
			}
		}


		prepared.trace(startTime, updateCount);
		return updateCount;
	}


	public boolean isReadOnly() {
		return prepared.isReadOnly();
	}

	public LocalResult queryMeta() throws SQLException {
		return prepared.queryMeta();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#acquireLocks()
	 */
	@Override
	public QueryProxy acquireLocks(QueryProxyManager queryProxyManager2) throws SQLException {
		return prepared.acquireLocks(proxyManager);
	}

	/**
	 * @return
	 */
	public  String getTableName(){
		return prepared.table.getFullName();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#shouldBePropagated()
	 */
	@Override
	public boolean shouldBePropagated() {
		return prepared.shouldBePropagated();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#addQueryProxyManager(org.h2.h2o.comms.QueryProxyManager)
	 */
	@Override
	public void addQueryProxyManager(QueryProxyManager proxyManager) {
		if (proxyManager == null) return;
		this.proxyManager = proxyManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.CommandInterface#getQueryProxyManager()
	 */
	@Override
	public QueryProxyManager getQueryProxyManager() {
		return proxyManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.CommandInterface#isPreparedStatement(boolean)
	 */
	@Override
	public void setIsPreparedStatement(boolean preparedStatement) {
		prepared.setPreparedStatement(preparedStatement);
	}
}
