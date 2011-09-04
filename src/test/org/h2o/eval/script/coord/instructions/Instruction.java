package org.h2o.eval.script.coord.instructions;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

public interface Instruction extends Serializable {

    public void execute(CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException, SQLException;

}
