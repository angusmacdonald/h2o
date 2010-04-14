package org.h2.h2o.manager;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MovedException extends Exception {

	private static final long serialVersionUID = -3440292190623018795L;

	/**
	 * @param newSystemTableLocation
	 */
	public MovedException(String newSystemTableLocation) {
		super(newSystemTableLocation);
	}
}
