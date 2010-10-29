/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.viewer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.h2.engine.Database;
import org.h2.util.NetUtils;

import uk.ac.standrews.cs.nds.events.IEvent;
import uk.ac.standrews.cs.nds.events.bus.interfaces.IEventConsumer;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class H2OEventConsumer implements IEventConsumer {

    private Socket socket;

    private ObjectOutputStream out;

    private boolean interested = true;

    private final Database database;

    public H2OEventConsumer(final Database database) {

        this.database = database;
    }

    @Override
    public boolean interested(final IEvent event) {

        return interested && event.getType().equals(H2OEventBus.H2O_EVENT);
    }

    @Override
    public void receiveEvent(final IEvent event) {

        final Object obj = event.get(H2OEventBus.H2O_EVENT);

        try {
            try {
                getConnection();
            }
            catch (final UnknownHostException e) {
                ErrorHandling.errorNoEvent("Event server not running. Events will be disabled.");
                interested = false;
                return;
            }

            out.writeObject(obj);
            out.flush();
        }
        catch (final IOException e) {
            ErrorHandling.errorNoEvent("Event server not connected. Events will be disabled.");
            interested = false;
            return;
        }
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (socket != null) {
                    socket.close();
                }
            }
            catch (final IOException e) {
            }
        }

    }

    private void getConnection() throws UnknownHostException, IOException {

        final String host = database.getDatabaseSettings().get("EVENT_SERVER_LOCATION");

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Trying to connect to event server at: " + host);

        System.err.println(NetUtils.getLocalAddress());
        socket = new Socket(host, 4444);
        out = new ObjectOutputStream(socket.getOutputStream());
    }
}
