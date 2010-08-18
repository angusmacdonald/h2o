package org.h2o.autonomic.decision.filter;

import java.util.Set;

import org.h2o.autonomic.decision.DatabaseMonitoringWrapper;

public interface IDatabaseFilter {

	public  Set<DatabaseMonitoringWrapper> filterUnsuitableDatabases(Set<DatabaseMonitoringWrapper> databaseInstances, Requirements requirements);
}
