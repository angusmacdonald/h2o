package org.h2o.eval.script.coord;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    private static final Pattern create_table_pattern = Pattern.compile("\\{create_table id=\"(\\d+)\"(?:\\s+name=\"([^\"]+)\")(?:\\s+schema=\"(.[^\"]+)\")(?:\\s+prepopulate_with=\"(\\d+)\")?\\}");

    public static void main(final String[] args) {

        final String test = "{create_table id=\"1\" name=\"test0\" schema=\"id int, id int, str_a varchar(40), int_a BIGINT\" prepopulate_with=\"300\"}";
        final String test2 = " name=\"test0\"";
        final String schemae = " schema=\"id int, id int, str_a varchar(40), int_a BIGINT\"";
        final String prepa = " prepopulate_with=\"300\"}";

        final Matcher matcher = create_table_pattern.matcher(test);
        if (matcher.matches()) {
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));

        }
        else {
            System.out.println("no");
        }

    }
}
