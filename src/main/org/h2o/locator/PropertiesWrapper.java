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
package org.h2o.locator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PropertiesWrapper {

	protected Properties properties;
	protected String propertiesFileLocation;
	protected FileOutputStream fos;


	protected static final String DATABASENAME = "databaseName";
	protected static final String CREATIONDATE = "creationDate";

	/**
	 * 
	 */
	public PropertiesWrapper() {
		super();
	}


	public void createPropertiesFile(){
		/*
		 * Create the properties file.
		 */
		File f = new File(propertiesFileLocation);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			fos = new FileOutputStream(propertiesFileLocation);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}


	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
