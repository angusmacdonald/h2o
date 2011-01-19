/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.util.ScriptReader;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * This class represents the statement RUNSCRIPT
 */
public class RunScriptCommand extends ScriptBase {

    private String charset = SysProperties.FILE_ENCODING;

    public RunScriptCommand(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    @Override
    public int update() throws SQLException {

        session.getUser().checkAdmin();
        int count = 0;
        try {
            openInput();
            final Reader reader = new InputStreamReader(in, charset);
            final ScriptReader r = new ScriptReader(new BufferedReader(reader));
            while (true) {
                final String sql = r.readStatement();
                if (sql == null) {
                    break;
                }
                execute(sql);
                count++;
            }
            reader.close();
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, null);
        }
        finally {
            closeIO();
        }
        return count;
    }

    private void execute(final String sql) throws SQLException {

        try {
            final Prepared command = session.prepare(sql);
            if (command.isQuery()) {
                command.query(0);
            }
            else {
                command.update();
            }
            if (session.getApplicationAutoCommit()) {
                session.commit(false);
            }
        }
        catch (final SQLException e) {
            throw Message.addSQL(e, sql);
        }
        catch (final RPCException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void setCharset(final String charset) {

        this.charset = charset;
    }

    @Override
    public LocalResult queryMeta() {

        return null;
    }

}
