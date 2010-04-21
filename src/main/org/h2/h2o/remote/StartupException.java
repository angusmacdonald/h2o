package org.h2.h2o.remote;

/**
 * Thrown when the database encounters an H2O specific error during startup.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class StartupException extends Exception {

	/**
	 * 
	 */
	public StartupException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public StartupException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public StartupException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public StartupException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
