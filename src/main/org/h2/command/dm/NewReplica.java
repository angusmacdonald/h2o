package org.h2.command.dm;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.CreateReplica;
import org.h2.command.ddl.SchemaCommand;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Constants;
import org.h2.engine.DataManager;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SchemaManager;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexType;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableLinkConnection;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * Represents the NEW REPLICA command, sent to a machine hosting the data manager for a given table. The syntax of the command is:
 * 
 * NEW REPLICA <tableName> (<databaseLocationOnDisk>, <hostname>, <port>, <connectionType>, <tableSet>);
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class NewReplica extends SchemaCommand {
	private String tableName;

	private int tableSet = -1; //the set of tables which this replica will belong to.

	private String databaseLocationOnDisk;
	private String hostname;
	private int port;

	private String connectionType;

	public NewReplica(Session session, Schema schema, String tableName, Expression[] expr) {
		super(session, schema);

		this.tableName = tableName;

		//NEW REPLICA <tableName> (<databaseLocationOnDisk>, <hostname>, <port>, <connectionType>, <tableSet>);


		
		//Parse EXPR
		this.databaseLocationOnDisk =expr[0].toString();
		if (databaseLocationOnDisk.startsWith("'") && databaseLocationOnDisk.endsWith("'")){
			databaseLocationOnDisk = databaseLocationOnDisk.substring(1, databaseLocationOnDisk.length()-1);
		}
		
		this.hostname = expr[1].toString();
		if (hostname.startsWith("'") && hostname.endsWith("'")){
			hostname = hostname.substring(1, hostname.length()-1);
		}
		this.port = new Integer(expr[2].toString()).intValue();
		this.connectionType = expr[3].toString();
		if (connectionType.startsWith("'") && connectionType.endsWith("'")){
			connectionType = connectionType.substring(1, connectionType.length()-1);
		}
		this.tableSet = new Integer(expr[4].toString()).intValue();
		

	}


	public int update() throws SQLException {

		Database db = session.getDatabase();



		// TODO rights: what rights are required to create a table?
		session.commit(true);


		try{
			//	#############################
			//  Add to data manager.
			//	#############################	
			DataManager dm = db.getDataManager(getSchema().getName() + "." + tableName);


			//NEW REPLICA <tableName> (<databaseLocationOnDisk>, <hostname>, <port>, <connectionType>, <tableSet>);
			dm.addReplicaInformation(0, databaseLocationOnDisk, "TABLE", hostname, port, connectionType, tableSet);



		} catch (SQLException e) {
			db.checkPowerOff();
			throw e;
		}

		return 0;
	}

	/**
	 * Set the tableSet number for this table.
	 * @param tableSet2
	 */
	private void setTableSet(int tableSet) {
		this.tableSet = tableSet;
	}

}
