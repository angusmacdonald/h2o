/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.event.client;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.h2.util.NetUtils;

import uk.ac.standrews.cs.nds.eventModel.IEvent;
import uk.ac.standrews.cs.nds.eventModel.eventBus.EventBus;
import uk.ac.standrews.cs.nds.eventModel.eventBus.busInterfaces.IEventBus;
import uk.ac.standrews.cs.nds.eventModel.eventBus.busInterfaces.IEventConsumer;

public class H2OEventConsumer implements IEventConsumer {
	IEventBus bus = new EventBus();

	private Socket socket;

	private ObjectOutputStream out;

	public H2OEventConsumer() {
		try {
			getConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean interested(IEvent event) {
		return event.getType().equals(H2OEventBus.H2O_EVENT);
	}

	@Override
	public void receiveEvent(IEvent event) {
		Object obj = event.get(H2OEventBus.H2O_EVENT);

		if (!socket.isConnected()) {
			try {
				getConnection();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			out.writeObject(obj);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void getConnection() throws UnknownHostException, IOException {
		socket = new Socket(NetUtils.getLocalAddress(), 4444);
		out = new ObjectOutputStream(socket.getOutputStream());
	}
}
