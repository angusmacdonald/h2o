package org.h2o.autonomic.decision.ranker.metric;

public class CreateReplicaRequest extends ActionRequest {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1444487511985378183L;
	
	private static double cpu = 0.1;
	
	private static double memory = 0.1;
	
	private static double network = 0.9;
	
	public CreateReplicaRequest(long expectedTimeToCompletion, long immediateDiskSpace, long expectedDiskSpace) {
		super(expectedTimeToCompletion, immediateDiskSpace, expectedDiskSpace, cpu, memory, network);
	}
	
}
