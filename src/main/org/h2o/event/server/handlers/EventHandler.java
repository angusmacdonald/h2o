package org.h2o.event.server.handlers;

import org.h2o.event.client.H2OEvent;

public interface EventHandler {

	public boolean pushEvent(H2OEvent event);

}