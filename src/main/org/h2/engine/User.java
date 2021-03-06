/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.security.SHA256;
import org.h2.table.MetaTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.ByteUtils;
import org.h2.util.ObjectArray;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * Represents a user object.
 */
public class User extends RightOwner {

    private final boolean systemUser;

    private byte[] salt;

    private byte[] passwordHash;

    private boolean admin;

    private int sessions = 0;

    public User(final Database database, final int id, final String userName, final boolean systemUser) {

        super(database, id, userName, Trace.USER);
        this.systemUser = systemUser;
    }

    public void setAdmin(final boolean admin) {

        this.admin = admin;
    }

    public boolean getAdmin() {

        return admin;
    }

    public synchronized void incrementSessionCount() {

        sessions++;
    }

    public synchronized void decrementSessionCount() {

        sessions--;
    }

    public int getSessionCount() {

        return sessions;
    }

    /**
     * Set the salt and hash of the password for this user.
     * 
     * @param salt
     *            the salt
     * @param hash
     *            the password hash
     */
    public void setSaltAndHash(final byte[] salt, final byte[] hash) {

        this.salt = salt;
        passwordHash = hash;
    }

    /**
     * Set the user name password hash. A random salt is generated as well. The parameter is filled with zeros after use.
     * 
     * @param userPasswordHash
     *            the user name password hash
     */
    public void setUserPasswordHash(final byte[] userPasswordHash) {

        if (userPasswordHash != null) {
            salt = RandomUtils.getSecureBytes(Constants.SALT_LEN);
            final SHA256 sha = new SHA256();
            passwordHash = sha.getHashWithSalt(userPasswordHash, salt);
        }
    }

    @Override
    public String getCreateSQLForCopy(final Table table, final String quotedName) {

        throw Message.throwInternalError();
    }

    @Override
    public String getCreateSQL() {

        return getCreateSQL(true, false);
    }

    @Override
    public String getDropSQL() {

        return null;
    }

    /**
     * Checks that this user has the given rights for this database object.
     * 
     * @param table
     *            the database object
     * @param rightMask
     *            the rights required
     * @throws SQLException
     *             if this user does not have the required rights
     */
    public void checkRight(final Table table, final int rightMask) throws SQLException {

        if (rightMask != Right.SELECT && !systemUser) {
            database.checkWritingAllowed();
        }
        if (admin) { return; }
        final Role publicRole = database.getPublicRole();
        if (publicRole.isRightGrantedRecursive(table, rightMask)) { return; }
        if (table instanceof MetaTable || table instanceof RangeTable) {
            // everybody has access to the metadata information
            return;
        }
        final String tableType = table.getTableType();
        if (Table.VIEW.equals(tableType)) {
            final TableView v = (TableView) table;
            if (v.getOwner() == this) {
                // the owner of a view has access:
                // SELECT * FROM (SELECT * FROM ...)
                return;
            }
        }
        else if (tableType == null) {
            // function table
            return;
        }
        if (!isRightGrantedRecursive(table, rightMask)) {
            if (table.getTemporary() && !table.getGlobalTemporary()) {
                // the owner has all rights on local temporary tables
                return;
            }
            throw Message.getSQLException(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    /**
     * Get the CREATE SQL statement for this object.
     * 
     * @param password
     *            true if the password (actually the salt and hash) should be returned
     * @param ifNotExists
     *            true if IF NOT EXISTS should be used
     * @return the SQL statement
     */
    public String getCreateSQL(final boolean password, final boolean ifNotExists) {

        final StringBuilder buff = new StringBuilder();
        buff.append("CREATE USER ");
        if (ifNotExists) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        if (password) {
            buff.append(" SALT '");
            buff.append(ByteUtils.convertBytesToString(salt));
            buff.append("' HASH '");
            buff.append(ByteUtils.convertBytesToString(passwordHash));
            buff.append("'");
        }
        else {
            buff.append(" PASSWORD ''");
        }
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    /**
     * Check the password of this user.
     * 
     * @param userPasswordHash
     *            the password data (the user password hash)
     * @return true if the user password hash is correct
     */
    public boolean validateUserPasswordHash(final byte[] userPasswordHash) {

        final SHA256 sha = new SHA256();
        final byte[] hash = sha.getHashWithSalt(userPasswordHash, salt);
        return ByteUtils.compareSecure(hash, passwordHash);
    }

    /**
     * Check if this user has admin rights. An exception is thrown if he does not have them.
     * 
     * @throws SQLException
     *             if this user is not an admin
     */
    public void checkAdmin() throws SQLException {

        if (!admin) { throw Message.getSQLException(ErrorCode.ADMIN_RIGHTS_REQUIRED); }
    }

    @Override
    public int getType() {

        return DbObject.USER;
    }

    @Override
    public ObjectArray getChildren() {

        ObjectArray all = database.getAllRights();
        final ObjectArray children = new ObjectArray();
        for (int i = 0; i < all.size(); i++) {
            final Right right = (Right) all.get(i);
            if (right.getGrantee() == this) {
                children.add(right);
            }
        }
        all = database.getAllSchemas();
        for (int i = 0; i < all.size(); i++) {
            final Schema schema = (Schema) all.get(i);
            if (schema.getOwner() == this) {
                children.add(schema);
            }
        }
        return children;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        final ObjectArray rights = database.getAllRights();
        for (int i = 0; i < rights.size(); i++) {
            final Right right = (Right) rights.get(i);
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        database.removeMeta(session, getId());
        salt = null;
        ByteUtils.clear(passwordHash);
        passwordHash = null;
        invalidate();
    }

    @Override
    public void checkRename() {

        // ok
    }

    /**
     * Check that this user does not own any schema. An exception is thrown if he owns one or more schemas.
     * 
     * @throws SQLException
     *             if this user owns a schema
     */
    public void checkOwnsNoSchemas() throws SQLException {

        if (database == null) { return; }
        final ObjectArray schemas = database.getAllSchemas();
        for (int i = 0; i < schemas.size(); i++) {
            final Schema s = (Schema) schemas.get(i);
            if (this == s.getOwner()) { throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, new String[]{getName(), s.getName()}); }
        }
    }

}
