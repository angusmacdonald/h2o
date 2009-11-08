/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * Represents a list of SQL statements.
 */
public class CommandList extends Command {

	private final Command command;
	private String[] remaining;
	private QueryProxyManager proxyManager;

	// TODO lock if possible!

	public CommandList(Parser parser, String sql, Command c, String remaining){
		super(parser, sql);
		this.command = c;

		/*
		 * Split and store remaining commands.
		 */
		if (remaining != null){
			this.remaining = remaining.split(";"); //TODO not particuarly safe. i.e. no query can contain a semi-colon.
		}

		this.proxyManager = new QueryProxyManager(parser.getSession().getDatabase(), getSession());
		command.addQueryProxyManager(proxyManager);

	}

	public ObjectArray getParameters() {
		return command.getParameters();
	}

	public int executeUpdate() throws SQLException {
    	return executeUpdate(true);
    }
	
	private SQLException executeRemaining() throws SQLException {
		SQLException rollbackException = null;

		if (remaining != null){
			try {
				/*
				 * H2O. Iterate through remaining commands rather than recursively calling this method.
				 */
				for (String sqlStatement: remaining){
					Command remainingCommand = session.prepareLocal(sqlStatement);
					remainingCommand.addQueryProxyManager(proxyManager);
					
					if (remainingCommand.isQuery()) {
						remainingCommand.query(0, true);
					} else {
						remainingCommand.update(true);
					}
				}

			} catch (SQLException e){
				rollbackException = e;
			}
		}


		return rollbackException;


	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#update(boolean)
	 */
	@Override
	protected int update(boolean partOfMultiQueryTransaction) throws SQLException {
		assert partOfMultiQueryTransaction;
		
		return update();
	}
	
	public int update() throws SQLException {
		/*
		 * Execute the first update, then iterate through every subsequent update.
		 */
		int updateCount = command.executeUpdate(true);
		SQLException rollbackException = executeRemaining();

		commit(rollbackException);

		return updateCount;
	}


	public LocalResult query(int maxrows) throws SQLException {

		LocalResult result = command.query(maxrows);
		SQLException rollbackException = executeRemaining();

		commit(rollbackException);

		return result;
	}
	
	/**
	 * Commit or rollback a transaction based on whether an exception was thrown during the update/query.
	 * @param rollbackException	Exception thrown during query. Will be null if none was thrown and transaction was successful.
	 * @throws SQLException
	 */
	private void commit(SQLException rollbackException)  throws SQLException {
		
		/*
		 * Having executed all commands, rollback if there was an exception. Otherwise, commit.
		 */
		proxyManager.commit(rollbackException == null);

		/*
		 * If we did a rollback, rethrow the exception that caused this to happen.
		 */
		if (rollbackException != null){
			throw rollbackException;
		}
	}

	public boolean isQuery() {
		return command.isQuery();
	}

	public boolean isTransactional() {
		return true;
	}

	public boolean isReadOnly() {
		return false;
	}

	public LocalResult queryMeta() throws SQLException {
		return command.queryMeta();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#acquireLocks()
	 */
	@Override
	public QueryProxy acquireLocks() throws SQLException {
		return command.acquireLocks();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#shouldBePropagated()
	 */
	@Override
	public boolean shouldBePropagated() {
		return command.shouldBePropagated();
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Command#addQueryProxyManager(org.h2.h2o.comms.QueryProxyManager)
	 */
	@Override
	protected void addQueryProxyManager(QueryProxyManager proxyManager) {
		ErrorHandling.hardError("Didn't expect this to be called.");
	}


}
