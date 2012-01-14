package org.h2o.db.manager;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.interfaces.ITableManagerRemote;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class NotifyTableManagersOfFailureAsync implements Runnable {

    private final ITableManagerRemote tm;
    private final DatabaseID machine;

    public NotifyTableManagersOfFailureAsync(final ITableManagerRemote tm, final DatabaseID failedMachine) {

        this.tm = tm;
        machine = failedMachine;
    }

    @Override
    public final void run() {

        try {
            tm.notifyOfFailure(machine);
        }
        catch (final RPCException e) {
            e.printStackTrace();
        }
    }

}
