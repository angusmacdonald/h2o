package org.h2o.db.manager.recovery;

public class LocatorException extends Exception {

	public LocatorException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 */
	public LocatorException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public LocatorException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public LocatorException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1959475152817687774L;

}
