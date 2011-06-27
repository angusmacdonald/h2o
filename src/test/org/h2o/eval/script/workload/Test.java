package org.h2o.eval.script.workload;

public class Test {

    /**
     * @param args
     */
    public static void main(final String[] args) {

        final int total = 100;

        final int size = 3;

        for (int i = 0; i < total; i++) {
            System.out.println(i % size);
        }
    }

}
