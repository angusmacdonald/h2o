/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.URI;

import org.h2.constant.SysProperties;

/**
 * This tool starts the browser with a specific URL.
 */
public class StartBrowser {

    private StartBrowser() {

        // utility class
    }

    /**
     * Open a new browser tab or window with the given URL.
     * 
     * @param url
     *            the URL to open
     */
    public static void openURL(String url) {

        String osName = SysProperties.getStringSetting("os.name", "linux").toLowerCase();
        Runtime rt = Runtime.getRuntime();
        try {
            String browser = SysProperties.BROWSER;
            if (browser != null) {
                if (osName.indexOf("windows") >= 0) {
                    rt.exec(new String[]{"cmd.exe", "/C", browser, url});
                }
                else {
                    rt.exec(new String[]{browser, url});
                }
                return;
            }

            try {
                Class desktopClass = Class.forName("java.awt.Desktop");
                // Desktop.isDesktopSupported()
                Boolean supported = (Boolean) desktopClass.getMethod("isDesktopSupported", new Class[0]).invoke(null, new Object[0]);
                URI uri = new URI(url);
                if (supported.booleanValue()) {
                    // Desktop.getDesktop();
                    Object desktop = desktopClass.getMethod("getDesktop", new Class[0]).invoke(null, new Object[0]);
                    // desktop.browse(uri);
                    desktopClass.getMethod("browse", new Class[]{URI.class}).invoke(desktop, new Object[]{uri});
                    return;
                }
            }
            catch (Exception e) {
                // ignore
            }

            if (osName.indexOf("windows") >= 0) {
                rt.exec(new String[]{"rundll32", "url.dll,FileProtocolHandler", url});
            }
            else if (osName.indexOf("mac") >= 0) {
                Runtime.getRuntime().exec(new String[]{"open", url});
            }
            else {
                String[] browsers = {"firefox", "mozilla-firefox", "mozilla", "konqueror", "netscape", "opera"};
                boolean ok = false;
                for (String browser2 : browsers) {
                    try {
                        rt.exec(new String[]{browser2, url});
                        ok = true;
                        break;
                    }
                    catch (Exception e) {
                        // ignore and try the next
                    }
                }
                if (!ok) {
                    // No success in detection.
                    System.out.println("Please open a browser and go to " + url);
                }
            }
        }
        catch (IOException e) {
            System.out.println("Failed to start a browser to open the URL " + url);
            e.printStackTrace();
        }
    }

}
