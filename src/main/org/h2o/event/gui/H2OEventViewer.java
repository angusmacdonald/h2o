package org.h2o.event.gui;

import javax.swing.JFrame;
import javax.swing.WindowConstants;

import org.h2o.event.DatabaseStates;
import org.h2o.event.client.H2OEvent;
import org.h2o.event.server.EventServer;
import org.h2o.event.server.handlers.EventHandler;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class H2OEventViewer implements EventHandler {

	private EventActions action;
	private EventServer server;

	/**
	 * Auto-generated main method to display this 
	 * JPanel inside a new JFrame.
	 */
	public static void main(String[] args) {
		Diagnostic.setLevel(DiagnosticLevel.FULL);

		AdvancedEventGui gui = new AdvancedEventGui();
		JFrame frame = new JFrame();
		frame.getContentPane().add(gui);
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		frame.pack();
		frame.setVisible(true);

		H2OEventViewer newViewer = new H2OEventViewer(gui);
		newViewer.start();
	}

	public H2OEventViewer(EventActions action) {
		this.action = action;
		
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		server = new EventServer(EventServer.EVENT_SERVER_PORT, this);
		

	}

	public void start(){
		server.start();
	}
	
	@Override
	public boolean pushEvent(H2OEvent event) {

		System.out.println(event);

		DatabaseStates state = (event.getEventType());

		try {

			switch (state){
			case DATABASE_STARTUP: action.databaseStartup(event); break;
			case TABLE_CREATION:
			case REPLICA_CREATION: action.replicaCreation(event); break;
			case TABLE_MANAGER_CREATION: action.tableManagerCreation(event); break;
			case TABLE_MANAGER_MIGRATION: action.tableManagerMigration(event); break;
			case TABLE_DELETION: action.tableDeletion(event);
			case TABLE_MANAGER_SHUTDOWN: action.tableManagerShutdown(event); break;
			case DATABASE_FAILURE: action.databaseFailure(event); break;
			case DATABASE_SHUTDOWN: action.databaseShutdown(event); break;
			case REPLICA_DELETION: action.replicaDeletion(event); break;
			case SYSTEM_TABLE_CREATION: action.systemTableCreation(event); break;
			case SYSTEM_TABLE_MIGRATION: action.systemTableMigration(event); break;
			case META_TABLE_REPLICA_CREATION: action.metaTableReplicaCreation(event); break;
			case TABLE_UPDATE: action.tableUpdate(event); break;
			case TABLE_WRITE: action.tableWrite(event); break;
			}


		} catch(NullPointerException e){
			e.printStackTrace();
		}

		return true;
	}
}
