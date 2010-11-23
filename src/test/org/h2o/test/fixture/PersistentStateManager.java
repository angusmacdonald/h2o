package org.h2o.test.fixture;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

public class PersistentStateManager {

    private static final int MAX_ATTEMPTS_TO_DELETE_PERSISTENT_STATE = 5;

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

        for (int i = 0; i < MAX_ATTEMPTS_TO_DELETE_PERSISTENT_STATE; i++) {

            deleteDatabaseDirectoriesIfPresent();
            deleteConfigDirectoryIfPresent();

            if (persistentStateIsAbsent()) { return; }

            try {
                Thread.sleep(1000);
            }
            catch (final InterruptedException e) {
                // Ignore.
            }
        }
        fail("couldn't delete persistent state");
    }

    private boolean persistentStateIsAbsent() {

        return databaseDirectoriesAreAbsent() && configDirectoryIsAbsent();
    }

    // -------------------------------------------------------------------------------------------------------

    private void deleteDatabaseDirectoriesIfPresent() {

        try {
            for (final File database_directory : database_directories) {
                delete(database_directory);
            }
        }
        catch (final IOException e) {
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

    private boolean databaseDirectoriesAreAbsent() {

        for (final File database_directory : database_directories) {
            if (database_directory.exists()) { return false; }
        }
        return true;
    }

    private boolean configDirectoryIsAbsent() {

        return !config_directory.exists();
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
