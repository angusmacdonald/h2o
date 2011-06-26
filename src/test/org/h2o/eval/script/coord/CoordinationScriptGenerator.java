package org.h2o.eval.script.coord;

public class CoordinationScriptGenerator {

    private static final String TABLE_NAME_PREFIX = "test";

    private static int nextMachineID = 0;
    private static int nextTableID = 0;

    public static String generateCoordinationScript(final long runtime, final double probabilityOfFailure, final double frequencyOfFailure, final int numberOfMachines, final int numberOfTables) {

        final StringBuilder script = new StringBuilder();

        startMachines(numberOfMachines, script);

        createTables(numberOfTables, script);

        specifyWorkloadsToExecute(runtime, script);

        //TODO how do you specify the tables and workloads you want to create.

        return script.toString();
    }

    /**
     * Specify what workloads will run on what machines.
     * @param runtime How long the workloads should run for.
     * @param script
     */
    private static void specifyWorkloadsToExecute(final long runtime, final StringBuilder script) {

        // TODO Auto-generated method stub

    }

    public static void createTables(final int numberOfTables, final StringBuilder script) {

        for (int i = 0; i < numberOfTables; i++) { //creates all tables on the first machine.
            script.append(createTableCommand(0, sqlSimpleCreateTable(TABLE_NAME_PREFIX + nextTableID())));
        }
    }

    public static void startMachines(final int numberOfMachines, final StringBuilder script) {

        for (int i = 0; i < numberOfMachines; i++) {
            script.append(createStartMachineCommand(nextMachineID(), i == 0));
        }
    }

    private static String createStartMachineCommand(final int id) {

        return createStartMachineCommand(id, false);
    }

    /**
     * 
     * @param id ID to be assigned to the starting machine.
     * @param sleepAfterStart Whether to sleep for a short period after starting the machine (can be used to ensure a machine becomes the system table).
     * @return
     */
    private static String createStartMachineCommand(final int id, final boolean sleepAfterStart) {

        String startMachineCommand = "{start_machine id=\"" + id + "\"}";

        if (sleepAfterStart) {
            startMachineCommand += "\n" + createSleepCommand(3000);
        }

        return startMachineCommand;
    }

    private static String createSleepCommand(final long sleepTime) {

        return "{sleep=\"" + sleepTime + "\"}";
    }

    private static String createTableCommand(final int id, final String sql) {

        return "{" + id + "} " + sql;
    }

    private static String executeWorkloadCommand(final int id, final String workloadLocation, final long duration) {

        return "{" + id + "} {execute_workload=\"" + workloadLocation + "\" duration=\"" + duration + "\"}";

    }

    private static String sqlSimpleCreateTable(final String tableName) {

        return "CREATE TABLE " + tableName + " (id int);";
    }

    private static String terminateMachineCommand(final int id) {

        return "{terminate_machine id=\"" + id + "\"}";
    }

    private static int nextMachineID() {

        return nextMachineID++;
    }

    private static int nextTableID() {

        return nextTableID++;
    }

}
