package org.h2.h2o.manager;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrationException extends Exception {

	private static final long serialVersionUID = -2549559616713278150L;

	public MigrationException(String message){
		super(message);
	}
}
