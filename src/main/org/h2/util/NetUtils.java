/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.security.SecureSocketFactory;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.NetworkUtil;

/**
 * This utility class contains socket helper functions.
 */
public class NetUtils {

    private static final int CACHE_MILLIS = 1000;

    private static InetAddress bindAddress;

    private static String cachedLocalAddress;

    private static long cachedLocalAddressTime;

    private NetUtils() {

        // utility class
    }

    /**
     * Create a loopback socket (a socket that is connected to localhost) on this port.
     * 
     * @param port
     *            the port
     * @param ssl
     *            if SSL should be used
     * @return the socket
     */
    public static Socket createLoopbackSocket(final int port, final boolean ssl) throws IOException {

        InetAddress address = getBindAddress();
        if (address == null) {
            address = InetAddress.getLocalHost();
        }
        return createSocket(address.getHostAddress(), port, ssl);
    }

    /**
     * Create a client socket that is connected to the given address and port.
     * 
     * @param server
     *            to connect to (including an optional port)
     * @param defaultPort
     *            the default port (if not specified in the server address)
     * @param ssl
     *            if SSL should be used
     * @return the socket
     */
    public static Socket createSocket(String server, final int defaultPort, final boolean ssl) throws IOException {

        int port = defaultPort;
        // IPv6: RFC 2732 format is '[a:b:c:d:e:f:g:h]' or
        // '[a:b:c:d:e:f:g:h]:port'
        // RFC 2396 format is 'a.b.c.d' or 'a.b.c.d:port' or 'hostname' or
        // 'hostname:port'
        final int startIndex = server.startsWith("[") ? server.indexOf(']') : 0;
        final int idx = server.indexOf(':', startIndex);
        if (idx >= 0) {
            port = MathUtils.decodeInt(server.substring(idx + 1));
            server = server.substring(0, idx);
        }
        final InetAddress address = InetAddress.getByName(server);
        return createSocket(address, port, ssl);
    }

    /**
     * Create a client socket that is connected to the given address and port.
     * 
     * @param address
     *            the address to connect to
     * @param port
     *            the port
     * @param ssl
     *            if SSL should be used
     * @return the socket
     */
    public static Socket createSocket(final InetAddress address, final int port, final boolean ssl) throws IOException {

        if (ssl) { return SecureSocketFactory.createSocket(address, port); }
        final Socket socket = new Socket();
        socket.connect(new InetSocketAddress(address, port), SysProperties.SOCKET_CONNECT_TIMEOUT);
        return socket;
    }

    /**
     * Create a server socket. The system property h2.bindAddress is used if set.
     * 
     * @param port
     *            the port to listen on
     * @param ssl
     *            if SSL should be used
     * @return the server socket
     */
    public static ServerSocket createServerSocketWithRetry(final int port, final boolean ssl) throws SQLException {

        final long startTime = System.currentTimeMillis();

        while (true) {

            try {
                return createServerSocket(port, ssl);
            }
            catch (final SQLException e) {
                // Wait and try again if timeout has not been exceeded.

                if (System.currentTimeMillis() - startTime > SysProperties.SERVER_SOCKET_RETRY_TIMEOUT) { throw e; }
                try {
                    Thread.sleep(SysProperties.SERVER_SOCKET_RETRY_WAIT);
                }
                catch (final InterruptedException e1) {
                    // Ignore and carry on.
                }
            }
        }
    }

    /**
     * Get the bind address if the system property h2.bindAddress is set, or null if not.
     * 
     * @return the bind address
     */
    private static InetAddress getBindAddress() throws UnknownHostException {

        final String host = SysProperties.BIND_ADDRESS;
        if (host == null || host.length() == 0) { return null; }
        synchronized (NetUtils.class) {
            if (bindAddress == null) {
                bindAddress = InetAddress.getByName(host);
            }
        }
        return bindAddress;
    }

    private static ServerSocket createServerSocket(final int port, final boolean ssl) throws SQLException {

        try {
            final InetAddress bindAddress = getBindAddress();
            if (ssl) { return SecureSocketFactory.createServerSocket(port, bindAddress); }
            if (bindAddress == null) { return NetworkUtil.makeReusableServerSocket(port); }
            return NetworkUtil.makeReusableServerSocket(bindAddress, port);
        }
        catch (final BindException be) {
            throw Message.getSQLException(ErrorCode.EXCEPTION_OPENING_PORT_2, new String[]{"" + port, be.toString()}, be);
        }
        catch (final IOException e) {
            throw Message.convertIOException(e, "port: " + port + " ssl: " + ssl);
        }
    }

    /**
     * Check if a socket is connected to a local address.
     * 
     * @param socket
     *            the socket
     * @return true if it is
     */
    public static boolean isLocalAddress(final Socket socket) throws UnknownHostException {

        final InetAddress test = socket.getInetAddress();
        // ## Java 1.4 begin ##
        if (test.isLoopbackAddress()) { return true; }
        // ## Java 1.4 end ##
        final InetAddress localhost = InetAddress.getLocalHost();
        // localhost.getCanonicalHostName() is very very slow
        final String host = localhost.getHostAddress();
        final InetAddress[] list = InetAddress.getAllByName(host);
        for (final InetAddress addr : list) {
            if (test.equals(addr)) { return true; }
        }
        return false;
    }

    /**
     * Close a server socket and ignore any exceptions.
     * 
     * @param socket
     *            the socket
     * @return null
     */
    public static ServerSocket closeSilently(final ServerSocket socket) {

        if (socket != null) {
            try {
                socket.close();
            }
            catch (final IOException e) {
                // ignore
            }
        }
        return null;
    }

    /**
     * Get the local host address as a string. For performance, the result is cached for one second.
     * 
     * @return the local host address
     */
    public static synchronized String getLocalAddress() {

        final long now = System.currentTimeMillis();
        if (cachedLocalAddress != null) {
            if (cachedLocalAddressTime + CACHE_MILLIS > now) { return cachedLocalAddress; }
        }
        InetAddress bind = null;
        try {
            bind = getBindAddress();
            if (bind == null) {
                bind = InetAddress.getLocalHost();
            }
        }
        catch (final UnknownHostException e) {
            // ignore
        }
        String address = bind == null ? "localhost" : bind.getHostAddress();
        if (address.equals("127.0.0.1")) {
            address = "localhost";
        }
        cachedLocalAddress = address;
        cachedLocalAddressTime = now;
        return address;
    }

}
