package org.h2o.util;

public class StringUtils {

    public static int countNumberOfCharacters(final String str, final String characterToCount) {

        return str.replaceAll("[^" + characterToCount + "]", "").length();
    }
}
