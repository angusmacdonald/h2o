/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.interfaces;

import org.h2o.autonomic.numonic.interfaces.ICentralDataCollector;
import org.h2o.db.interfaces.IH2ORemote;
import org.h2o.db.manager.util.IMigratable;

/**
 * The remote interface to the System Table. This is a basic extension of ISystemTableLocal
 * that requires that the implementing class also implements Migratable commands. Local system
 * table instances (in-memory/persisted) don't need to implement Migratable because they are not accessed
 * remotely, hence the distinction.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISystemTableMigratable extends ISystemTable, IMigratable, IH2ORemote, ICentralDataCollector {

}
