package org.h2o.test;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

public class PersistentStateManager {

    private final File[] database_directories;

    private final File config_directory;

    // -------------------------------------------------------------------------------------------------------

    public PersistentStateManager(final String config_directory_path, final String[] database_directory_paths) {

        config_directory = new File(config_directory_path);
        database_directories = new File[database_directory_paths.length];

        for (int i = 0; i < database_directory_paths.length; i++) {
            database_directories[i] = new File(database_directory_paths[i]);
        }
    }

    // -------------------------------------------------------------------------------------------------------

    public void deletePersistentState() {

        deleteDatabaseDirectoriesIfPresent();
        deleteConfigDirectoryIfPresent();
    }

    public void assertPersistentStateIsAbsent() {

        assertDatabaseDirectoriesAreAbsent();
        assertConfigDirectoryIsAbsent();
    }

    // -------------------------------------------------------------------------------------------------------

    private void deleteDatabaseDirectoriesIfPresent() {

        try {
            for (final File database_directory : database_directories) {
                delete(database_directory);
            }
        }
        catch (final IOException e) {
            e.printStackTrace();
            // Ignore.
        }
    }

    private void deleteConfigDirectoryIfPresent() {

        try {
            delete(config_directory);
        }
        catch (final IOException e) {
            // Ignore.
        }
    }

    private void assertDatabaseDirectoriesAreAbsent() {

        for (final File database_directory : database_directories) {
            assertFalse(database_directory.exists());
        }
    }

    private void assertConfigDirectoryIsAbsent() {

        assertFalse(config_directory.exists());
    }

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
