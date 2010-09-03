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
package org.h2o.event.gui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.Label;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.WindowConstants;
import javax.swing.border.TitledBorder;

import org.h2o.db.id.DatabaseURL;
import org.h2o.event.client.H2OEvent;


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
public class AdvancedEventGui extends javax.swing.JPanel implements EventActions {

	private static final long serialVersionUID = 4823972128575647792L;

	private Map<DatabaseURL, JPanel> dbs = new HashMap<DatabaseURL, JPanel>();

	private Map<String, Label> tableManagers = new HashMap<String, Label>();

	private Map<String, Map<DatabaseURL, Label>> replicas = new HashMap<String, Map<DatabaseURL, Label>>();

	private Label systemTable = null;

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

	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableManagerMigration(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableManagerMigration(H2OEvent event) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableWrite(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableWrite(H2OEvent event) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableUpdate(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableUpdate(H2OEvent event) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#metaTableReplicaCreation(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void metaTableReplicaCreation(H2OEvent event) {
		// TODO Auto-generated method stub
		
	}

	public void databaseStartup(H2OEvent event) {
		JPanel panel = createDatabasePanel(event);

		this.add(panel);

		dbs.put(event.getDatabase(), panel);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#replicaCreation(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void replicaCreation(H2OEvent event) {
		JPanel dbPanel = getPanel(event);

		Label l = setTableReplica(event, dbPanel);

		addTableToReplicas(event, l);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableManagerCreation(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableManagerCreation(H2OEvent event) {
		JPanel dbPanel;
		Label l;
		dbPanel = getPanel(event);
		l = new Label("Table Manager: " + event.getEventValue());
		l.setBackground(Color.YELLOW);

		tableManagers.put(event.getEventValue(), l);

		dbPanel.add(l);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#systemTableMigration(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void systemTableMigration(H2OEvent event) {
		JPanel dbPanel;
		dbPanel = getPanel(event);

		systemTablePanel.remove(systemTable);

		setSystemTable(dbPanel);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#systemTableCreation(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void systemTableCreation(H2OEvent event) {
		JPanel dbPanel;
		dbPanel = getPanel(event);

		setSystemTable(dbPanel);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#replicaDeletion(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void replicaDeletion(H2OEvent event) {
		Map<DatabaseURL, Label> replicaLocations;
		JPanel dbPanel;
		Label l;
		dbPanel = getPanel(event);

		replicaLocations = replicas.get(event.getEventValue());
		if (replicaLocations != null){
			l = replicaLocations.remove(event.getDatabase());
			if (l != null) dbPanel.remove(l);
		}
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#databaseShutdown(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void databaseShutdown(H2OEvent event) {
		JPanel dbPanel;
		dbPanel = getPanel(event);
		dbPanel.setBackground(Color.BLACK);
		dbPanel.setBorder(new TitledBorder("Database (Inactive): " + event.getDatabase().getDbLocation()));
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#databaseFailure(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void databaseFailure(H2OEvent event) {
		JPanel dbPanel;
		dbPanel = getPanel(event);
		dbPanel.setBackground(Color.RED);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableManagerShutdown(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableManagerShutdown(H2OEvent event) {
		JPanel dbPanel;
		dbPanel = getPanel(event);
		dbPanel.remove(tableManagers.get(event.getEventValue()));
		tableManagers.remove(event.getEventValue());
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#tableDeletion(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public void tableDeletion(H2OEvent event) {
		JPanel dbPanel;
		Map<DatabaseURL, Label> replicaLocations = replicas.get(event.getEventValue());


		for (Entry<DatabaseURL, Label> location: replicaLocations.entrySet()){
			dbPanel = getPanel(location.getKey());
			dbPanel.remove(location.getValue());
		}
	}

	public void addTableToReplicas(H2OEvent event, Label l) {

		Map<DatabaseURL, Label> replicasForTable = replicas.remove(event.getEventValue());

		if (replicasForTable == null) replicasForTable = new HashMap<DatabaseURL, Label>();

		replicasForTable.put(event.getDatabase(), l);

		replicas.put(event.getEventValue(), replicasForTable);
	}

	/* (non-Javadoc)
	 * @see org.h2o.event.gui.EventActions#createDatabasePanel(org.h2o.event.client.H2OEvent)
	 */
	@Override
	public JPanel createDatabasePanel(H2OEvent event) {
		JPanel panel = new JPanel(true);
		panel.setVisible(true);
		panel.setBorder(new TitledBorder("Database: " + event.getDatabase().getDbLocation()));
		return panel;
	}

	public JPanel getPanel(H2OEvent event) {
		JPanel dbPanel = dbs.get(event.getDatabase());

		if (dbPanel == null) return createDatabasePanel(event);

		return dbPanel;
	}

	public JPanel getPanel(DatabaseURL db) {
		JPanel dbPanel = dbs.get(db);

		return dbPanel;
	}

	public Label setTableReplica(H2OEvent event, JPanel dbPanel) {
		Label l;
		l = new Label("Table Replica: " + event.getEventValue());
		l.setBackground(Color.BLUE);
		l.setForeground(Color.WHITE);
		dbPanel.add(l);
		return l;
	}

	public void setSystemTable(JPanel dbPanel) {
		Label l;
		l = new Label("SYSTEM TABLE");
		l.setBackground(Color.GREEN);
		l.setForeground(Color.WHITE);
		dbPanel.add(l);

		systemTable = l;
		systemTablePanel = dbPanel;
	}

}
