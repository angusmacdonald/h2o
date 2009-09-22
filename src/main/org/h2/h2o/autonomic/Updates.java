package org.h2.h2o.autonomic;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class Updates {
	/**
	 * Whether updates in the system are propagated synchronously (to all replicas at the same time), or asynchronously (to one replica initially, then
	 * eventually to the rest.
	 */
	public static boolean SYNCHRONOUS_UPDATE = true;
}
