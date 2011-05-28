package org.h2o.autonomic.numonic;

/**
 * Used by local components to determine whether they are able to continue operation. For example, if the instance is not connected to the System Table,
 * then it shouldn't try to send monitoring information, or to execute queries. 
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface ISystemStatus {

    /**
     * Indicates whether this database instance is connected to an H2O database system.
     * @return true if it is connected and able to execute queries, false if it isn't.
     */
    public boolean isConnected();

    /**
     * Update information on whether this database system is connected to an H2O database.
     * @param connected  true if it is connected and able to execute queries, false if it isn't.
     */
    public void setConnected(boolean connected);
}
