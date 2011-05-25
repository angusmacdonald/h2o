package org.h2o.autonomic.numonic;

import java.io.InputStream;

public class PropertiesFileLoader {

    public InputStream getResource(final String name) {

        final ClassLoader classLoader = this.getClass().getClassLoader();
        return classLoader.getResourceAsStream(name);
    }
}
