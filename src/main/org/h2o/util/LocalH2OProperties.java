/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util;


import java.io.File;
import java.io.IOException;

import org.h2o.db.id.DatabaseURL;

import uk.ac.standrews.cs.nds.util.H2OPropertiesWrapper;

/**
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocalH2OProperties extends H2OPropertiesWrapper {

	/**
	 * @param dbURL
	 *            The URL of this database instance. This is used to name and locate the properties file for this database on disk.
	 * @param appendum
	 *            A string to be added on to the DBurl as part of the properties file name.
	 */
	public LocalH2OProperties(DatabaseURL dbURL) {

		super("config" + File.separator + dbURL.sanitizedLocation() + ".properties");
	}

	/**
	 * @param string
	 */
	public LocalH2OProperties(String descriptorLocation) {
		super("config" + File.separator + descriptorLocation + ".properties");
	}

	@Override
	public void createNewFile() {
		try {
			super.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void saveAndClose() {
		try {
			super.saveAndClose();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
}
