package org.h2o.event.gui;
import java.awt.Canvas;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.Label;
import java.awt.Rectangle;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import javax.swing.JButton;

import javax.swing.WindowConstants;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import org.h2o.db.id.DatabaseURL;
import org.h2o.event.DatabaseStates;
import org.h2o.event.client.H2OEvent;
import org.h2o.event.server.EventServer;
import org.h2o.event.server.handlers.EventHandler;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;


/**
 * This code was edited or generated using CloudGarden's Jigloo
 * SWT/Swing GUI Builder, which is free for non-commercial
 * use. If Jigloo is being used commercially (ie, by a corporation,
 * company or business for any purpose whatever) then you
 * should purchase a license for each developer using Jigloo.
 * Please visit www.cloudgarden.com for details.
 * Use of Jigloo implies acceptance of these licensing terms.
 * A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
 * THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
 * LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
 */
public class AdvancedEventGui extends javax.swing.JPanel implements EventHandler {

	private static final long serialVersionUID = 4823972128575647792L;

	private Map<DatabaseURL, JPanel> dbs = new HashMap<DatabaseURL, JPanel>();

	private Map<String, Label> tableManagers = new HashMap<String, Label>();

	private Map<String, Map<DatabaseURL, Label>> replicas = new HashMap<String, Map<DatabaseURL, Label>>();

	private Label systemTable = null;

	private Queue<H2OEvent> events = new LinkedList<H2OEvent>();

	private JPanel systemTablePanel;

	/**
	 * Auto-generated main method to display this 
	 * JPanel inside a new JFrame.
	 */
	public static void main(String[] args) {

		JFrame frame = new JFrame();
		frame.getContentPane().add(new AdvancedEventGui());
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		frame.pack();
		frame.setVisible(true);
	}

	public AdvancedEventGui() {
		setPreferredSize(new Dimension(400, 300));
		GridLayout thisLayout = new GridLayout(1, 1);
		thisLayout.setHgap(5);
		thisLayout.setVgap(5);
		thisLayout.setColumns(1);
		this.setLayout(thisLayout);


		Diagnostic.setLevel(DiagnosticLevel.FULL);
		EventServer server = new EventServer(EventServer.EVENT_SERVER_PORT, this);
		server.start();

	}

	@Override
	public boolean pushEvent(H2OEvent event) {

		events.add(event);

		System.out.println(event);
		
		DatabaseStates state = (event.getEventType());

		try {
			
		
		switch (state){
		case DATABASE_STARTUP:
			JPanel panel = createDatabasePanel(event);

			this.add(panel);

			dbs.put(event.getDatabase(), panel);
			break;

		case TABLE_CREATION:
		case REPLICA_CREATION:
			JPanel dbPanel = getPanel(event);

			Label l = setTableReplica(event, dbPanel);

			addTableToReplicas(event, l);
			break;
	
		case TABLE_MANAGER_CREATION:
			dbPanel = getPanel(event);
			l = new Label("Table Manager: " + event.getEventValue());
			l.setBackground(Color.YELLOW);

			tableManagers.put(event.getEventValue(), l);

			dbPanel.add(l);
			break;
		case TABLE_MANAGER_MIGRATION:
			break;
		case TABLE_MANAGER_REPLICA_CREATION:
			break;
		case TABLE_DELETION:
			
			Map<DatabaseURL, Label> replicaLocations = replicas.get(event.getEventValue());
			
			
			for (Entry<DatabaseURL, Label> location: replicaLocations.entrySet()){
				dbPanel = getPanel(location.getKey());
				dbPanel.remove(location.getValue());
			}
			
		case TABLE_MANAGER_SHUTDOWN:
			dbPanel = getPanel(event);
			dbPanel.remove(tableManagers.get(event.getEventValue()));
			tableManagers.remove(event.getEventValue());
			break;
		case DATABASE_FAILURE:
			dbPanel = getPanel(event);
			dbPanel.setBackground(Color.RED);
			break;
		case DATABASE_SHUTDOWN:
			dbPanel = getPanel(event);
			dbPanel.setBackground(Color.BLACK);
			dbPanel.setBorder(new TitledBorder("Database (Inactive): " + event.getDatabase().getDbLocation()));
			break;
		case REPLICA_DELETION:
			dbPanel = getPanel(event);

			replicaLocations = replicas.get(event.getEventValue());
			
			l = replicaLocations.remove(event.getDatabase());
			if (l != null) dbPanel.remove(l);

			break;
		case SYSTEM_TABLE_CREATION:
			dbPanel = getPanel(event);

			setSystemTable(dbPanel);
			break;
		case SYSTEM_TABLE_MIGRATION:
			dbPanel = getPanel(event);

			systemTablePanel.remove(systemTable);
			
			setSystemTable(dbPanel);
			break;
		case SYSTEM_TABLE_REPLICA_CREATION:
			//TODO implement.
			break;
		case TABLE_UPDATE:
			//TODO implement.
			break;
		case TABLE_WRITE:
			//TODO implement.
			break;
		default:
			System.err.println(state + " not found.");
		}

		
		} catch(NullPointerException e){
			e.printStackTrace();
		}
		
		repaint();

		return true;
	}

	private void addTableToReplicas(H2OEvent event, Label l) {

		Map<DatabaseURL, Label> replicasForTable = replicas.remove(event.getEventValue());
		
		if (replicasForTable == null) replicasForTable = new HashMap<DatabaseURL, Label>();
		
		replicasForTable.put(event.getDatabase(), l);
		
		replicas.put(event.getEventValue(), replicasForTable);
	}

	private JPanel createDatabasePanel(H2OEvent event) {
		JPanel panel = new JPanel(true);
		panel.setVisible(true);
		panel.setBorder(new TitledBorder("Database: " + event.getDatabase().getDbLocation()));
		return panel;
	}

	private JPanel getPanel(H2OEvent event) {
		JPanel dbPanel = dbs.get(event.getDatabase());
		
		if (dbPanel == null) return createDatabasePanel(event);
		
		return dbPanel;
	}
	
	private JPanel getPanel(DatabaseURL db) {
		JPanel dbPanel = dbs.get(db);
	
		return dbPanel;
	}

	private Label setTableReplica(H2OEvent event, JPanel dbPanel) {
		Label l;
		l = new Label("Table Replica: " + event.getEventValue());
		l.setBackground(Color.BLUE);
		l.setForeground(Color.WHITE);
		dbPanel.add(l);
		return l;
	}

	private void setSystemTable(JPanel dbPanel) {
		Label l;
		l = new Label("SYSTEM TABLE");
		l.setBackground(Color.GREEN);
		l.setForeground(Color.WHITE);
		dbPanel.add(l);

		systemTable = l;
		systemTablePanel = dbPanel;
	}

	private String getTableKey(H2OEvent event) {
		return event.getDatabase() + event.getEventValue();
	}

}
