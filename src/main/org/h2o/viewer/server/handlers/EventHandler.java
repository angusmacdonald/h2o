package org.h2o.viewer.server.handlers;

import org.h2o.viewer.client.H2OEvent;

public interface EventHandler {
	
	public boolean pushEvent(H2OEvent event);
	
}