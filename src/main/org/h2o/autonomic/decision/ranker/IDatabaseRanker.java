package org.h2o.autonomic.decision.ranker;

import java.util.Queue;
import java.util.Set;

import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public interface IDatabaseRanker {
	public Queue<DatabaseInstanceWrapper> sortDatabaseInstances(Set<DatabaseInstanceWrapper> suitableDatabaseInstances, DatabaseSortingMetric requirements);
}
