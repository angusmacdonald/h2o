package org.h2o.autonomic.decision.filter;

import java.util.HashSet;
import java.util.Set;

import org.h2o.autonomic.decision.DatabaseMonitoringWrapper;

public class DatabaseFilter implements IDatabaseFilter {

	@Override
	public Set<DatabaseMonitoringWrapper> filterUnsuitableDatabases(Set<DatabaseMonitoringWrapper> databaseInstances, Requirements requirements) {
		
		Set<DatabaseMonitoringWrapper> suitableInstances = new HashSet<DatabaseMonitoringWrapper>();
		
		for (DatabaseMonitoringWrapper instance: databaseInstances){
			if (instance.meetsRequirements(requirements)) suitableInstances.add(instance);
		}
		
		return suitableInstances;
	}
}
