package org.h2o.db.query.asynchronous;

/**
 * Thread that continuously checks whether a set of queries have been completed.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class AsynchronousQueryCheckerThread extends Thread {

	/**
	 * The manager of all currently executing queries.
	 */
	AsynchronousQueryManager queryManager;
	
	private static final int SLEEP_TIME = 10;
	
	public AsynchronousQueryCheckerThread(AsynchronousQueryManager queryManager) {
		this.queryManager = queryManager;
	}

	public void run() {
		
		while (true){
			
			try { Thread.sleep(SLEEP_TIME); } catch (InterruptedException e) { }
			
			queryManager.checkForCompletion();
		}
	}
}
