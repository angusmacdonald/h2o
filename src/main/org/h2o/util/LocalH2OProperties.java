/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util;

import java.io.File;
import java.io.IOException;

import org.h2o.db.id.DatabaseURL;

/**
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocalH2OProperties extends H2OPropertiesWrapper {

    public static final String DEFAULT_CONFIG_DIRECTORY = "config";

    /**
     * @param dbURL the URL of this database instance. This is used to name and locate the properties file for this database on disk.
     */
    public LocalH2OProperties(final DatabaseURL dbURL) {

        this(getConfigurationDirectoryPath(dbURL.getDbLocation()), dbURL.sanitizedLocation());
    }

    //    public LocalH2OProperties(final String configDirectoryPath, final DatabaseURL dbURL) {
    //
    //        this(configDirectoryPath, dbURL.sanitizedLocation());
    //    }

    //    public LocalH2OProperties(final String propertiesFileName) {
    //
    //        this(DEFAULT_CONFIG_DIRECTORY, propertiesFileName);
    //    }

    public LocalH2OProperties(final String configDirectoryPath, final String propertiesFileName) {

        super(configDirectoryPath + File.separator + propertiesFileName + ".properties");
    }

    public static String getConfigurationDirectoryPath(final String databaseBaseDirectoryPath, final String databaseName, final String port) {

        return databaseBaseDirectoryPath + File.separator + databaseName + port + "." + DEFAULT_CONFIG_DIRECTORY;
    }

    public static String getConfigurationDirectoryPath(final String databaseDirectoryPath) {

        return databaseDirectoryPath + "." + DEFAULT_CONFIG_DIRECTORY;
    }

    @Override
    public void createNewFile() {

        try {
            super.createNewFile();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveAndClose() {

        try {
            super.saveAndClose();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }
    }
}
