package org.h2.constant;

/**
 * Specifies where the user would like a query to be directed (i.e. to which replica.)
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public enum LocationPreference {
	LOCAL, PRIMARY, NO_PREFERENCE
}
