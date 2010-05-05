package org.h2.h2o.util.locator.messages;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LockRequestResponse {
	private int updateCount;
	private boolean successful;
	/**
	 * @param updateCount
	 * @param successful
	 */
	public LockRequestResponse(int updateCount, boolean successful) {
		super();
		this.updateCount = updateCount;
		this.successful = successful;
	}
	/**
	 * @return the updateCount
	 */
	public synchronized int getUpdateCount() {
		return updateCount;
	}
	/**
	 * @return the successful
	 */
	public synchronized boolean isSuccessful() {
		return successful;
	}
	
	
}
