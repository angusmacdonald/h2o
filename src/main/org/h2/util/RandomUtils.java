/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Utility class that supports random and secure random functions. In some systems SecureRandom initialization is very slow, a workaround is
 * implemented here.
 */
public class RandomUtils {

    /**
     * The secure random object.
     */
    static SecureRandom cachedSecureRandom;

    /**
     * True if the secure random object is seeded.
     */
    static volatile boolean seeded;

    private static final Random RANDOM = new Random();

    private RandomUtils() {

        // utility class
    }

    private static synchronized SecureRandom getSecureRandom() {

        if (cachedSecureRandom != null) { return cachedSecureRandom; }
        // Workaround for SecureRandom problem as described in
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6202721
        // Can not do that in a static initializer block, because
        // threads are not started until after the initializer block exits
        try {
            cachedSecureRandom = SecureRandom.getInstance("SHA1PRNG");
            // On some systems, secureRandom.generateSeed() is very slow.
            // In this case it is initialized using our own seed implementation
            // and afterwards (in the thread) using the regular algorithm.
            Runnable runnable = new Runnable() {

                public void run() {

                    try {
                        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
                        byte[] seed = sr.generateSeed(20);
                        synchronized (cachedSecureRandom) {
                            cachedSecureRandom.setSeed(seed);
                            seeded = true;
                        }
                    }
                    catch (NoSuchAlgorithmException e) {
                        warn("SecureRandom", e);
                    }
                }
            };
            Thread t = new Thread(runnable);
            // let the process terminate even if generating the seed is really
            // slow
            t.setDaemon(true);
            t.start();
            Thread.yield();
            try {
                // normally, generateSeed takes less than 200 ms
                t.join(400);
            }
            catch (InterruptedException e) {
                warn("InterruptedException", e);
            }
            if (!seeded) {
                byte[] seed = generateAlternativeSeed();
                // this never reduces randomness
                synchronized (cachedSecureRandom) {
                    cachedSecureRandom.setSeed(seed);
                }
            }
        }
        catch (NoSuchAlgorithmException e) {
            warn("SecureRandom", e);
            cachedSecureRandom = new SecureRandom();
        }
        return cachedSecureRandom;
    }

    private static byte[] generateAlternativeSeed() {

        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            // milliseconds
            out.writeLong(System.currentTimeMillis());

            // nanoseconds if available
            try {
                Method m = System.class.getMethod("nanoTime", new Class[0]);
                if (m != null) {
                    Object o = m.invoke(null, (java.lang.Object[]) null);
                    out.writeUTF(o.toString());
                }
            }
            catch (Exception e) {
                // nanoTime not found, this is ok (only exists for JDK 1.5 and
                // higher)
            }

            // memory
            out.writeInt(new Object().hashCode());
            Runtime runtime = Runtime.getRuntime();
            out.writeLong(runtime.freeMemory());
            out.writeLong(runtime.maxMemory());
            out.writeLong(runtime.totalMemory());

            // environment
            try {
                out.writeUTF(System.getProperties().toString());
            }
            catch (Exception e) {
                warn("generateAlternativeSeed", e);
            }

            // host name and ip addresses (if any)
            try {
                String hostName = InetAddress.getLocalHost().getHostName();
                out.writeUTF(hostName);
                InetAddress[] list = InetAddress.getAllByName(hostName);
                for (InetAddress element : list) {
                    out.write(element.getAddress());
                }
            }
            catch (Exception e) {
                // on some system, InetAddress.getLocalHost() doesn't work
                // for some reason (incorrect configuration)
            }

            // timing (a second thread is already running)
            for (int j = 0; j < 16; j++) {
                int i = 0;
                long end = System.currentTimeMillis();
                while (end == System.currentTimeMillis()) {
                    i++;
                }
                out.writeInt(i);
            }

            out.close();
            return bout.toByteArray();
        }
        catch (IOException e) {
            warn("generateAlternativeSeed", e);
            return new byte[1];
        }
    }

    /**
     * Get a cryptographically secure pseudo random long value.
     * 
     * @return the random long value
     */
    public static long getSecureLong() {

        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            return sr.nextLong();
        }
    }

    /**
     * Get a number of cryptographically secure pseudo random bytes.
     * 
     * @param len
     *            the number of bytes
     * @return the random bytes
     */
    public static byte[] getSecureBytes(int len) {

        if (len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            sr.nextBytes(buff);
        }
        return buff;
    }

    /**
     * Get a pseudo random int value between 0 (including and the given value (excluding). The value is not cryptographically secure.
     * 
     * @param lowerThan
     *            the value returned will be lower than this value
     * @return the random long value
     */
    public static int nextInt(int lowerThan) {

        return RANDOM.nextInt(lowerThan);
    }

    /**
     * Get a cryptographically secure pseudo random int value between 0 (including and the given value (excluding).
     * 
     * @param lowerThan
     *            the value returned will be lower than this value
     * @return the random long value
     */
    public static int nextSecureInt(int lowerThan) {

        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            return sr.nextInt(lowerThan);
        }
    }

    /**
     * Print a message to system output if there was a problem initializing the random number generator.
     * 
     * @param s
     *            the message to print
     * @param t
     *            the stack trace
     */
    static void warn(String s, Throwable t) {

        // not a fatal problem, but maybe reduced security
        System.out.println("RandomUtils warning: " + s);
        if (t != null) {
            t.printStackTrace();
        }
    }

}
