package org.h2o.test;

import static org.junit.Assert.assertEquals;

import org.h2o.util.StringUtils;
import org.junit.Test;

public class UtilityTests {

    @Test
    public void testQuestionMarkCount() {

        final String twoQs = "??";
        final String twoQsSpaces = "? ?";
        final String twoQsOtherChars = "? hello there ?";

        assertEquals(2, StringUtils.countNumberOfCharacters(twoQs, "?"));
        assertEquals(2, StringUtils.countNumberOfCharacters(twoQsSpaces, "?"));
        assertEquals(2, StringUtils.countNumberOfCharacters(twoQsOtherChars, "?"));

        final String noQs = "void test";

        assertEquals(0, StringUtils.countNumberOfCharacters(noQs, "?"));

        final String tenQs = "??????????";
        final String tenQsSpaces = "? this ? this ? this ? this ? this ? this ? this ? this ? this ? this ";

        assertEquals(10, StringUtils.countNumberOfCharacters(tenQs, "?"));
        assertEquals(10, StringUtils.countNumberOfCharacters(tenQsSpaces, "?"));

    }
}
