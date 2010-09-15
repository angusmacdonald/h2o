package org.h2o.db.manager.recovery;

public class SystemTableAccessException extends Exception {

	private static final long serialVersionUID = 2118856587957923414L;

	public SystemTableAccessException() {
	}

	public SystemTableAccessException(String message) {
		super(message);
	}

	public SystemTableAccessException(Throwable cause) {
		super(cause);
	}

	public SystemTableAccessException(String message, Throwable cause) {
		super(message, cause);
	}

}
