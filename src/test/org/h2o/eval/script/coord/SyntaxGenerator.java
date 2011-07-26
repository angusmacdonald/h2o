package org.h2o.eval.script.coord;

public class SyntaxGenerator {

    /**
     * 
     * @param id ID to be assigned to the starting machine.
     * @param sleepAfterStart Whether to sleep for a short period after starting the machine (can be used to ensure a machine becomes the system table).
     * @return
     */
    protected static String createStartMachineCommand(final int id, final boolean sleepAfterStart) {

        String startMachineCommand = "{start_machine id=\"" + id + "\"}\n";

        if (sleepAfterStart) {
            startMachineCommand += SyntaxGenerator.createSleepCommand(3000);
        }

        return startMachineCommand;
    }

    public static String createReserveMachineCommand(final int id) {

        return "{reserve_machine id=\"" + id + "\"}\n";

    }

    public static String createStartReservedMachineCommand(final int id, final boolean sleepBeforeStart) {

        String command = "";

        if (sleepBeforeStart) {
            command += SyntaxGenerator.createSleepCommand(10000);
        }

        return command + "{start_reserved_machine id=\"" + id + "\"}\n";

    }

    protected static String createSleepCommand(final long sleepTime) {

        return "{sleep=\"" + sleepTime + "\"}\n";
    }

    protected static String createTableCommand(final int id, final String sql) {

        return "{" + id + "} " + sql;
    }

    protected static String executeWorkloadCommand(final int id, final String workloadLocation, final long duration) {

        return "{" + id + "} {execute_workload=\"" + workloadLocation + "\" duration=\"" + duration + "\"}\n";

    }

    protected static String sqlSimpleCreateTable(final String tableName) {

        return "CREATE TABLE " + tableName + " (id int);\n";
    }

    protected static String sqlComplexCreateTable(final String tableName) {

        //<loop-counter/>, <generated-string/>, <generated-string/>, <generated-string/>, <generated-string/>, <generated-long/>, <generated-long/>

        return "CREATE TABLE " + tableName + " (id int, str_a varchar(40), str_b varchar(40), str_c varchar(40), str_d varchar(40), int_a BIGINT, int_b BIGINT);\n";
    }

    protected static String terminateMachineCommand(final int id) {

        return "{terminate_machine id=\"" + id + "\"}\n";
    }

}
