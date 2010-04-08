package org.h2.h2o.util.filter;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface Predicate<T, P> { 
	boolean apply(T type, P parameter); 
	
}
