package org.h2o.eval;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

/**
 * Methods needed to start a co-ordination script.
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 *
 */
public interface ICoordinatorScript {

    public void setupSystem(int locatorServerPort, int tableReplicationFactor, int metadataReplicationFactor) throws IOException, StartupException;

    public void executeCoordinatorScript(final String coordinationScriptLocation, final String resultsFolderLocation) throws FileNotFoundException, IOException, WorkloadParseException, StartupException, SQLException, WorkloadException;

    public void executeCoordinationScript(List<String> script, String resultsFolderLocation, String coordinationScriptLocation) throws WorkloadParseException, RemoteException, StartupException, SQLException, WorkloadException;

    public void blockUntilWorkloadsComplete(Integer timeSlicePeriod) throws RemoteException;;

}
