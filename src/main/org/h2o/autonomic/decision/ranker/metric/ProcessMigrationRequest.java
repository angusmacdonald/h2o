package org.h2o.autonomic.decision.ranker.metric;

public class ProcessMigrationRequest extends ActionRequest {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -342488210144252900L;
	
	private static double cpu = 0.1;
	
	private static double memory = 0.1;
	
	private static double network = 0.9;
	
	public ProcessMigrationRequest(long expectedTimeToCompletion, long immediateDiskSpace, long expectedDiskSpace) {
		super(expectedTimeToCompletion, immediateDiskSpace, expectedDiskSpace, cpu, memory, network);
	}
}
