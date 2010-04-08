package org.h2.h2o.util.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;



/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class CollectionFilter {
	
	/**
	 * 
	 * @param <T>			The type of the objects being iterated over.
	 * @param <P>			The parameter to be passed in as part of a test.
	 * @param target		The target of this collection filter. A collection of obejcts of type T.
	 * @param predicate		The function to apply to each object in the connection.
	 * @param parameter		The parameter to be used as part of the test.
	 * @return				The set of elements of target which meet the requirements of the predicate function.
	 */
	public static <T, P> Set<T> filter(Collection<T> target, Predicate<T, P> predicate, P parameter) {
		Set<T> result = new HashSet<T>();
	    
		for (T element: target) {
	        if (predicate.apply(element, parameter)) {
	            result.add(element);
	        }
	    }
	    return result;
	}

}
