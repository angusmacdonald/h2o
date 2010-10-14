package org.h2o.autonomic.decision.ranker.metric;

public class CreateTableRequest extends ActionRequest {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5354604333488160485L;
	
	private static long expectedTimeToCompletion = 10;
	
	private static long immediateDiskSpace = 0;
	
	private static double cpu = 0.1;
	
	private static double memory = 0.1;
	
	private static double network = 0.9;
	
	public CreateTableRequest(long expectedDiskSpace) {
		super(expectedTimeToCompletion, immediateDiskSpace, expectedDiskSpace, cpu, memory, network);
	}
	
}
