/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.store.FileLister;
import org.h2.store.fs.FileSystem;
import org.h2.util.FileUtils;
import org.h2.util.Tool;

/**
 * Delete the database files. The database must be closed before calling this tool.
 */
public class DeleteDbFiles extends Tool {

    private void showUsage() {

        out.println("Deletes all files belonging to a database.");
        out.println("java " + getClass().getName() + "\n" + " [-dir <dir>]      The directory (default: .)\n" + " [-db <database>]  The database name\n" + " [-quiet]          Do not print progress information");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into strings like this: "-db", "test",... Options are case
     * sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)</li>
     * <li>-dir database directory (the default is the current directory)</li>
     * <li>-db database name (all databases if no name is specified)</li>
     * <li>-quiet does not print progress information</li>
     * </ul>
     * 
     * @param args
     *            the command line arguments
     * @throws SQLException
     */
    public static void main(final String[] args) throws SQLException {

        new DeleteDbFiles().run(args);
    }

    @Override
    public void run(final String[] args) throws SQLException {

        String dir = ".";
        String db = null;
        boolean quiet = false;
        for (int i = 0; args != null && i < args.length; i++) {
            final String arg = args[i];
            if (arg.equals("-dir")) {
                dir = args[++i];
            }
            else if (arg.equals("-db")) {
                db = args[++i];
            }
            else if (arg.equals("-quiet")) {
                quiet = true;
            }
            else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            }
            else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        process(dir, db, quiet, null);
    }

    /**
     * Deletes the database files.
     * 
     * @param dir
     *            the directory
     * @param db
     *            the database name (null for all databases)
     * @param quiet
     *            don't print progress information
     * @throws SQLException
     */
    public static void execute(final String dir, final String db, final boolean quiet) throws SQLException {

        new DeleteDbFiles().process(dir, db, quiet, null);
    }

    public static void execute(final String dir, final String db, final boolean quiet, final Set<String> fileExtensions) throws SQLException {

        new DeleteDbFiles().process(dir, db, quiet, fileExtensions);
    }

    /**
     * Deletes the database files.
     * 
     * @param dir
     *            the directory
     * @param db
     *            the database name (null for all databases)
     * @param quiet
     *            don't print progress information
     * @param fileExtensions The file extensions that should be deleted. Null if all relevant database extensions should be deleted.
     * @throws SQLException
     */
    private void process(final String dir, final String db, final boolean quiet, final Set<String> fileExtensions) throws SQLException {

        final DeleteDbFiles delete = new DeleteDbFiles();
        final ArrayList files = FileLister.getDatabaseFiles(dir, db, true);
        if (files.size() == 0 && !quiet) {
            printNoDatabaseFilesFound(dir, db);
        }
        for (int i = 0; i < files.size(); i++) {
            final String fileName = (String) files.get(i);

            boolean deleteFile = true;

            if (fileExtensions != null && fileExtensions.size() > 0) {
                deleteFile = false;

                for (final String ext : fileExtensions) {
                    if (fileName.endsWith(ext)) {
                        deleteFile = true;
                    }
                }
            }

            if (deleteFile) {
                delete.process(fileName, quiet);
                if (!quiet) {
                    out.println("Processed: " + fileName);
                }
            }
        }
    }

    private void process(final String fileName, final boolean quiet) throws SQLException {

        if (FileUtils.isDirectory(fileName)) {
            try {
                FileSystem.getInstance(fileName).deleteRecursive(fileName);
            }
            catch (final SQLException e) {
                if (!quiet) { throw e; }
            }
        }
        else if (quiet || fileName.endsWith(Constants.SUFFIX_TEMP_FILE) || fileName.endsWith(Constants.SUFFIX_TRACE_FILE)) {
            FileUtils.tryDelete(fileName);
        }
        else {
            FileUtils.delete(fileName);
        }
    }

}
