/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.exceptions;

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
