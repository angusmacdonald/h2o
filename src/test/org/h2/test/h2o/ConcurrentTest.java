package org.h2.test.h2o;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ConcurrentTest extends Thread {

	private Statement stat;
	private int startNum;
	private int totalIterations;
	
	public boolean successful = true;

	public ConcurrentTest(Statement stat, int startNum, int totalIterations){
		this.stat = stat;
		this.startNum = startNum;
		this.totalIterations = totalIterations;
	}

	public void run(){
		for (int i = startNum; i < (totalIterations + startNum); i++){
			try {
				stat.execute("INSERT INTO TEST VALUES(" + i + ", 'hello');");
			} catch (SQLException e) {
				successful = false;
			}
		}
	}
}
