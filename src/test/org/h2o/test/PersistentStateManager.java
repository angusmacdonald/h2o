package org.h2o.test;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

public class PersistentStateManager {

    private final File database_directory;

    //    private final File config_directory;

    // -------------------------------------------------------------------------------------------------------

    public PersistentStateManager(final String database_directory_path) {

        database_directory = new File(database_directory_path);
        //        config_directory = new File(config_directory_path);
    }

    // -------------------------------------------------------------------------------------------------------

    public void deletePersistentState() {

        deleteDatabaseDirectoryIfPresent();
        //        deleteConfigDirectoryIfPresent();
    }

    public void assertPersistentStateIsAbsent() {

        assertDatabaseDirectoryIsAbsent();
        //        assertConfigDirectoryIsAbsent();
    }

    // -------------------------------------------------------------------------------------------------------

    private void deleteDatabaseDirectoryIfPresent() {

        try {
            delete(database_directory);
        }
        catch (final IOException e) {
            // Ignore.
        }
    }

    //    private void deleteConfigDirectoryIfPresent() {
    //
    //        try {
    //            delete(config_directory);
    //        }
    //        catch (final IOException e) {
    //            // Ignore.
    //        }
    //    }

    private void assertDatabaseDirectoryIsAbsent() {

        assertFalse(database_directory.exists());
    }

    //    private void assertConfigDirectoryIsAbsent() {
    //
    //        assertFalse(config_directory.exists());
    //    }

    private void delete(final File file) throws IOException {

        if (file.isDirectory()) {

            final String[] children = file.list();
            if (children == null) { throw new IOException("null directory listing"); }
            for (final String child : children) {
                delete(new File(file, child));
            }
        }

        if (!file.delete()) { throw new IOException("couldn't delete file " + file.getAbsolutePath()); }
    }
}
