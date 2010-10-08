package org.h2o.viewer.gui;

import javax.swing.JPanel;

import org.h2o.viewer.client.H2OEvent;

public interface EventActions {

	public void databaseStartup(H2OEvent event);

	public void tableManagerMigration(H2OEvent event);

	public void tableWrite(H2OEvent event);

	public void tableUpdate(H2OEvent event);

	public void metaTableReplicaCreation(H2OEvent event);

	public void replicaCreation(H2OEvent event);

	public void tableManagerCreation(H2OEvent event);

	public void systemTableMigration(H2OEvent event);

	public void systemTableCreation(H2OEvent event);

	public void replicaDeletion(H2OEvent event);

	public void databaseShutdown(H2OEvent event);

	public void databaseFailure(H2OEvent event);

	public void tableManagerShutdown(H2OEvent event);

	public void tableDeletion(H2OEvent event);

	public JPanel createDatabasePanel(H2OEvent event);

}