/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.Set;

import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;
import org.h2.table.TableData;

/**
 * This class represents the statement ANALYZE
 */
public class Analyze extends DefineCommand {

    private int sampleRows = Constants.SELECTIVITY_ANALYZE_SAMPLE_ROWS;

    public Analyze(Session session) {

        super(session);
    }

    public int update() throws SQLException {

        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        Set<ReplicaSet> replicaSet = db.getAllTables();
        // TODO do we need to lock the table?
        for (ReplicaSet replicas : replicaSet) {
            Table table = replicas.getACopy();
            if (!(table instanceof TableData)) {
                continue;
            }
            Column[] columns = table.getColumns();
            StringBuilder buff = new StringBuilder();
            buff.append("SELECT ");
            for (int j = 0; j < columns.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append("SELECTIVITY(");
                buff.append(columns[j].getSQL());
                buff.append(")");
            }
            buff.append(" FROM ");
            buff.append(table.getSQL());
            if (sampleRows > 0) {
                buff.append(" LIMIT 1 SAMPLE_SIZE ");
                buff.append(sampleRows);
            }
            String sql = buff.toString();
            Prepared command = session.prepare(sql);
            LocalResult result = command.query(0);
            result.next();
            for (int j = 0; j < columns.length; j++) {
                int selectivity = result.currentRow()[j].getInt();
                columns[j].setSelectivity(selectivity);
            }
            db.update(session, table);
        }
        return 0;
    }

    public void setTop(int top) {

        this.sampleRows = top;
    }

}
