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


	private Queue<H2OEvent> events = new LinkedList<H2OEvent>();

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

		DatabaseStates state = (event.getEventType());

		switch (state){
		case DATABASE_STARTUP:
			JPanel panel = new JPanel(true);
			panel.setVisible(true);
			panel.setBorder(new TitledBorder("Database: " + event.getDatabase().getDbLocation()));

			this.add(panel);

			dbs.put(event.getDatabase(), panel);
			break;

		case TABLE_CREATION:
			JPanel dbPanel = dbs.get(event.getDatabase());

			Label l = new Label("Table: " + event.getEventValue());
			l.setBackground(Color.BLUE);
			l.setForeground(Color.WHITE);
			dbPanel.add(l);
			break;
		case TABLE_DELETION:
			break;
		case TABLE_MANAGER_CREATION:
			dbPanel = dbs.get(event.getDatabase());
			l = new Label("Table Manager: " + event.getEventValue());
			l.setBackground(Color.YELLOW);

			tableManagers.put(event.getEventValue(), l);

			dbPanel.add(l);
			break;
		case TABLE_MANAGER_MIGRATION:
			break;
		case TABLE_MANAGER_REPLICA_CREATION:
			break;
		case TABLE_MANAGER_SHUTDOWN:
			System.err.println("shutdown");
			dbPanel = dbs.get(event.getDatabase());
			dbPanel.remove(tableManagers.get(event.getEventValue()));

			break;
		case TABLE_UPDATE:
			break;
		case TABLE_WRITE:
			break;
		case DATABASE_FAILURE:
			break;
		case DATABASE_SHUTDOWN:
			break;
		case REPLICA_CREATION:
			break;
		case REPLICA_DELETION:
			break;
		case SYSTEM_TABLE_CREATION:
			break;
		case SYSTEM_TABLE_MIGRATION:
			break;

		case SYSTEM_TABLE_REPLICA_CREATION:
			break;			
		default:
			System.err.println(state + " not found.");
		}

		repaint();

		return true;
	}

}
