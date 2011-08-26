/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * This file system stores files on disk. This is the most common file system.
 */
public class FileSystemDisk extends FileSystem {

    private static final FileSystemDisk INSTANCE = new FileSystemDisk();

    // TODO detection of 'case in sensitive filesystem'
    // could maybe implemented using some other means
    private static final boolean IS_FILE_SYSTEM_CASE_INSENSITIVE = File.separatorChar == '\\';

    protected FileSystemDisk() {

        // nothing to do
    }

    public static FileSystemDisk getInstance() {

        return INSTANCE;
    }

    @Override
    public long length(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).length();
    }

    /**
     * Translate the file name to the native format. This will expand the home directory (~).
     * 
     * @param fileName
     *            the file name
     * @return the native file name
     */
    protected String translateFileName(String fileName) {

        if (fileName != null && fileName.startsWith("~")) {
            final String userDir = SysProperties.USER_HOME;
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    @Override
    public void rename(String oldName, String newName) throws SQLException {

        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        final File oldFile = new File(oldName);
        final File newFile = new File(newName);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            Message.throwInternalError("rename file old=new");
        }
        if (!oldFile.exists()) { throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2, new String[]{oldName + " (not found)", newName}); }
        if (newFile.exists()) { throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2, new String[]{oldName, newName + " (exists)"}); }
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            trace("rename", oldName + " >" + newName, null);
            final boolean ok = oldFile.renameTo(newFile);
            if (ok) { return; }
            wait(i);
        }
        throw Message.getSQLException(ErrorCode.FILE_RENAME_FAILED_2, new String[]{oldName, newName});
    }

    /**
     * Print a trace message if tracing is enabled.
     * 
     * @param method
     *            the method
     * @param fileName
     *            the file name
     * @param o
     *            the object
     */
    protected void trace(final String method, final String fileName, final Object o) {

        if (SysProperties.TRACE_IO) {
            System.out.println("FileSystem." + method + " " + fileName + " " + o);
        }
    }

    private static void wait(final int i) {

        if (i > 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            final long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        }
        catch (final InterruptedException e) {
            // ignore
        }
    }

    @Override
    public boolean createNewFile(String fileName) {

        fileName = translateFileName(fileName);
        final File file = new File(fileName);
        for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            }
            catch (final IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    @Override
    public boolean exists(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).exists();
    }

    @Override
    public void delete(String fileName) throws SQLException {

        fileName = translateFileName(fileName);
        final File file = new File(fileName);
        if (file.exists()) {
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                trace("delete", fileName, null);
                final boolean ok = file.delete();
                if (ok) { return; }
                wait(i);
            }
            throw Message.getSQLException(ErrorCode.FILE_DELETE_FAILED_1, fileName);
        }
    }

    @Override
    public boolean tryDelete(String fileName) {

        fileName = translateFileName(fileName);
        trace("tryDelete", fileName, null);
        final File file = new File(fileName);
        if (!file.exists()) {
            System.err.println("File doesn't exist.");
        }
        return file.delete();
    }

    @Override
    public String createTempFile(String name, final String suffix, final boolean deleteOnExit, final boolean inTempDir) throws IOException {

        name = translateFileName(name);
        name += ".";
        String prefix = new File(name).getName();
        File dir;
        if (inTempDir) {
            dir = null;
        }
        else {
            dir = new File(name).getAbsoluteFile().getParentFile();
            dir.mkdirs();
        }
        if (prefix.length() < 3) {
            prefix += "0";
        }
        final File f = File.createTempFile(prefix, suffix, dir);
        if (deleteOnExit) {
            try {
                f.deleteOnExit();
            }
            catch (final Throwable e) {
                // sometimes this throws a NullPointerException
                // at java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                // we can ignore it
            }
        }
        return f.getCanonicalPath();
    }

    @Override
    public String[] listFiles(String path) throws SQLException {

        path = translateFileName(path);
        final File f = new File(path);
        try {
            final String[] list = f.list();
            if (list == null) { return new String[0]; }
            String base = f.getCanonicalPath();
            if (!base.endsWith(File.separator)) {
                base += File.separator;
            }
            for (int i = 0; i < list.length; i++) {
                list[i] = base + list[i];
            }
            return list;
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, path);
        }
    }

    @Override
    public void deleteRecursive(String fileName) throws SQLException {

        fileName = translateFileName(fileName);
        if (FileUtils.isDirectory(fileName)) {
            final String[] list = listFiles(fileName);
            for (int i = 0; list != null && i < list.length; i++) {
                deleteRecursive(list[i]);
            }
        }
        delete(fileName);
    }

    @Override
    public boolean isReadOnly(String fileName) {

        fileName = translateFileName(fileName);
        final File f = new File(fileName);
        return f.exists() && !f.canWrite();
    }

    @Override
    public String normalize(String fileName) throws SQLException {

        fileName = translateFileName(fileName);
        final File f = new File(fileName);
        try {
            return f.getCanonicalPath();
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }

    @Override
    public String getParent(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).getParent();
    }

    @Override
    public boolean isDirectory(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).isDirectory();
    }

    @Override
    public boolean isAbsolute(String fileName) {

        fileName = translateFileName(fileName);
        final File file = new File(fileName);
        return file.isAbsolute();
    }

    @Override
    public String getAbsolutePath(String fileName) {

        fileName = translateFileName(fileName);
        final File parent = new File(fileName).getAbsoluteFile();
        return parent.getAbsolutePath();
    }

    @Override
    public long getLastModified(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).lastModified();
    }

    @Override
    public boolean canWrite(String fileName) {

        fileName = translateFileName(fileName);
        return new File(fileName).canWrite();
    }

    @Override
    public void copy(String original, String copy) throws SQLException {

        original = translateFileName(original);
        copy = translateFileName(copy);
        OutputStream out = null;
        InputStream in = null;
        try {
            out = FileUtils.openFileOutputStream(copy, false);
            in = FileUtils.openFileInputStream(original);
            final byte[] buffer = new byte[Constants.IO_BUFFER_SIZE];
            while (true) {
                final int len = in.read(buffer);
                if (len < 0) {
                    break;
                }
                out.write(buffer, 0, len);
            }
            out.close();
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, "original: " + original + " copy: " + copy);
        }
        finally {
            IOUtils.closeSilently(in);
            IOUtils.closeSilently(out);
        }
    }

    @Override
    public void createDirs(String fileName) throws SQLException {

        fileName = translateFileName(fileName);
        final File f = new File(fileName);
        if (!f.exists()) {
            final String parent = f.getParent();
            if (parent == null) { return; }
            final File dir = new File(parent);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                if (dir.exists() || dir.mkdirs()) { return; }
                wait(i);
            }
            throw Message.getSQLException(ErrorCode.FILE_CREATION_FAILED_1, parent);
        }
    }

    @Override
    public String getFileName(String name) throws SQLException {

        name = translateFileName(name);
        final String separator = SysProperties.FILE_SEPARATOR;
        String path = getParent(name);
        if (!path.endsWith(separator)) {
            path += separator;
        }
        final String fullFileName = normalize(name);
        if (!fullFileName.startsWith(path)) {
            Message.throwInternalError("file utils error: " + fullFileName + " does not start with " + path);
        }
        final String fileName = fullFileName.substring(path.length());
        return fileName;
    }

    @Override
    public boolean fileStartsWith(String fileName, String prefix) {

        fileName = translateFileName(fileName);
        if (IS_FILE_SYSTEM_CASE_INSENSITIVE) {
            fileName = StringUtils.toUpperEnglish(fileName);
            prefix = StringUtils.toUpperEnglish(prefix);
        }
        return fileName.startsWith(prefix);
    }

    @Override
    public OutputStream openFileOutputStream(String fileName, final boolean append) throws SQLException {

        fileName = translateFileName(fileName);
        try {
            final File file = new File(fileName);
            createDirs(file.getAbsolutePath());
            final FileOutputStream out = new FileOutputStream(fileName, append);
            trace("openFileOutputStream", fileName, out);
            return out;
        }
        catch (final IOException e) {
            freeMemoryAndFinalize();
            try {
                return new FileOutputStream(fileName);
            }
            catch (final IOException e2) {
                throw Message.convertIOException(e, fileName);
            }
        }
    }

    @Override
    public InputStream openFileInputStream(String fileName) throws IOException {

        if (fileName.indexOf(':') > 1) {
            // if the : is in position 1, a windows file access is assumed: C:..
            // or D:
            // otherwise a URL is assumed
            final URL url = new URL(fileName);
            final InputStream in = url.openStream();
            return in;
        }
        fileName = translateFileName(fileName);
        final FileInputStream in = new FileInputStream(fileName);
        trace("openFileInputStream", fileName, in);
        return in;
    }

    /**
     * Call the garbage collection and run finalization. This close all files that were not closed, and are no longer referenced.
     */
    protected void freeMemoryAndFinalize() {

        trace("freeMemoryAndFinalize", null, null);
        final Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            final long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    @Override
    public FileObject openFileObject(String fileName, final String mode) throws IOException {

        fileName = translateFileName(fileName);
        FileObjectDisk f;
        try {
            f = new FileObjectDisk(fileName, mode);
            trace("openRandomAccessFile", fileName, f);
        }
        catch (final IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileObjectDisk(fileName, mode);
            }
            catch (final IOException e2) {
                e2.initCause(e);
                throw e2;
            }
        }
        return f;
    }

}
