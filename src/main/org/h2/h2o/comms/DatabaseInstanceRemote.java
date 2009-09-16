package org.h2.h2o.comms;


/**
 * Interface to a database instance. For each database instance in the H2O system there will be one DatabaseInstanceRemote
 * exposed via the system's RMI registry.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DatabaseInstanceRemote extends H2ORemote, TwoPhaseCommit  {

}