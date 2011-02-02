package org.h2o.util;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.h2.util.NetUtils;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class H2ONetUtils {

    /**
     * Find a TCP port that is available to be used by the database. Use the specified TCP port if possible,
     * but not if it is currently used by another server.
     * @param preferredTCPPort
     * @return an open TCP port.
     */
    public static int getInactiveTCPPort(final int preferredTCPPort) {

        Socket sock = null; //Attempt to create a socket connection to the specified port.

        int tcpPortToUse = preferredTCPPort; //The port we'll try to connect to. The upcoming loop will keep on attempting connections until an open port is found.

        boolean freePortFound = false;

        while (!freePortFound) {
            try {
                sock = new Socket(NetUtils.getLocalAddress(), tcpPortToUse);
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Can't use preferred TCP port " + tcpPortToUse + "");
                tcpPortToUse++;
            }
            catch (final UnknownHostException e) {
                ErrorHandling.errorNoEvent("Couldn't establish a local hostname. The database may start but it might try to start it's TCP server on a port" + " alread bound by another application.");
                freePortFound = true; //not found, but there's nothing we can do here if the hostname doesn't resolve.
            }
            catch (final IOException e) {
                freePortFound = true;
                //Nothing is bound to this port - it can be used for this database instance.
            }
            finally {
                try {
                    if (sock != null && !sock.isClosed()) {
                        sock.close();
                    }
                }
                catch (final IOException e) {
                    //Not important at this point.
                }

            }
        }

        return tcpPortToUse;
    }
}
