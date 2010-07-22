package org.h2.h2o.manager;

public class TableManagerAccessPinger extends Thread {

	private InMemorySystemTable inMemorySystemTable;

	private boolean running = true;
	private int sleepTime = 5000;
	
	public TableManagerAccessPinger(InMemorySystemTable inMemorySystemTable) {
		this.inMemorySystemTable = inMemorySystemTable;
	}

	public void run(){
		
		while (isRunning()){
			/*
			 * Sleep.
			 */
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			inMemorySystemTable.checkTableManagerAccessibility();
		}
	}
	
	
	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running ;
	}


	/**
	 * @param running the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

}
