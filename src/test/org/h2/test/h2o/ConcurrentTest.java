package org.h2.test.h2o;

import java.sql.Statement;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ConcurrentTest extends Thread {

	private Statement stat;
	private int startNum;
	private int totalIterations;

	public boolean successful = true;
	private boolean update;

	public ConcurrentTest(Statement stat, int startNum, int totalIterations, boolean update){
		this.stat = stat;
		this.startNum = startNum;
		this.totalIterations = totalIterations;
		this.update = update;
	}

	public void run(){
		for (int i = startNum; i < (totalIterations + startNum); i++){
			try {
				if (update){
					stat.execute("INSERT INTO TEST VALUES(" + i + ", 'hello');");
				} else {
					stat.execute("SELECT * FROM TEST;");
				}
			} catch (Exception e) {
				successful = false;
			}
		}
	}
}
