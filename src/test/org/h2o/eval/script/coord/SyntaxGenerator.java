package org.h2o.eval.script.coord;

public class SyntaxGenerator {

    protected static String createSleepCommand(final long sleepTime) {

        return "{sleep=\"" + sleepTime + "\"}";
    }

    protected static String createTableCommand(final int id, final String sql) {

        return "{" + id + "} " + sql;
    }

    protected static String executeWorkloadCommand(final int id, final String workloadLocation, final long duration) {

        return "{" + id + "} {execute_workload=\"" + workloadLocation + "\" duration=\"" + duration + "\"}";

    }

    protected static String sqlSimpleCreateTable(final String tableName) {

        return "CREATE TABLE " + tableName + " (id int);";
    }

    protected static String terminateMachineCommand(final int id) {

        return "{terminate_machine id=\"" + id + "\"}";
    }
}
