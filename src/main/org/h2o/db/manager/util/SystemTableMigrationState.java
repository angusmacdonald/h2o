/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.util;

import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

public class SystemTableMigrationState {

    /**
     * If this System Table has been moved to another location (i.e. its state has been transferred to another machine and it is no longer
     * active) this field will not be null, and will note the new location of the System Table.
     */
    public String movedLocation = null;

    /**
     * Whether the System Table is in the process of being migrated. If this is true the System Table will be 'locked', unable to service
     * requests.
     */
    public boolean inMigration = false;

    /**
     * Whether the System Table has been moved to another location.
     */
    public boolean hasMoved = false;

    /**
     * Whether the System Table has been shutdown.
     */
    public boolean shutdown = false;

    /**
     * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
     */
    public long migrationTime = 0l;

    public IChordRemoteReference location = null;

    public SystemTableMigrationState(IChordRemoteReference localChordReference) {

        location = localChordReference;
    }
}
