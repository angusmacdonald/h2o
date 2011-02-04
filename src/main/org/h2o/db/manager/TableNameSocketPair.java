package org.h2o.db.manager;

import java.net.InetSocketAddress;

public class TableNameSocketPair {

    private final InetSocketAddress proxy_address;
    private final String tableName;

    public TableNameSocketPair(final InetSocketAddress proxy_address, final String tableName) {

        this.proxy_address = proxy_address;
        this.tableName = tableName;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (proxy_address == null ? 0 : proxy_address.hashCode());
        result = prime * result + (tableName == null ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final TableNameSocketPair other = (TableNameSocketPair) obj;
        if (proxy_address == null) {
            if (other.proxy_address != null) { return false; }
        }
        else if (!proxy_address.equals(other.proxy_address)) { return false; }
        if (tableName == null) {
            if (other.tableName != null) { return false; }
        }
        else if (!tableName.equals(other.tableName)) { return false; }
        return true;
    }

}
