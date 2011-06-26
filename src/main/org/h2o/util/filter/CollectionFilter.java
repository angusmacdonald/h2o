/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.filter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class CollectionFilter {

    /**
     * Filter elements from a collection by checking a predicate against the key of each value.
     * @param <T>
     *            The type of the objects being iterated over.
     * @param <P>
     *            The parameter to be passed in as part of a test.
     * @param target
     *            The target of this collection filter. A collection of objects of type T.
     * @param predicate
     *            The function to apply to each object in the connection.
     * @param parameter
     *            The parameter to be used as part of the test.
     * @return The set of elements of target which meet the requirements of the predicate function.
     */
    public static <T, P> Set<T> filter(final Collection<T> target, final PredicateWithParameter<T, P> predicate, final P parameter) {

        final Set<T> result = new HashSet<T>();

        for (final T element : target) {
            if (predicate.apply(element, parameter)) {
                result.add(element);
            }
        }
        return result;
    }

    /**
     * Filter elements from a Map by checking a predicate against the key of each value.
     * @param <K> The type of the map's key.
     * @param <V> They type of the map's value.
     *            The type of the objects being iterated over.
     * @param target
     *            The target of this collection filter. A collection of objects of type T.
     * @param predicate
     *            The function to apply to each object in the connection.
     * @return The set of elements of target which meet the requirements of the predicate function.
     */
    public static <K, V> Map<K, V> filter(final Map<K, V> target, final PredicateWithoutParameter<K> predicate) {

        final Map<K, V> result = new HashMap<K, V>();

        for (final Entry<K, V> entry : target.entrySet()) {
            if (predicate.apply(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

}
