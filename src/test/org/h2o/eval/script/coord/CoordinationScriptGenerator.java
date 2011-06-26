package org.h2o.eval.script.coord;

import java.util.Set;

import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.WorkloadType;

public class CoordinationScriptGenerator {

    private static final String TABLE_NAME_PREFIX = "test";

    private static int nextMachineID = 0;
    private static int nextTableID = 0;

    public static String generateCoordinationScript(final long runtime, final double probabilityOfFailure, final double frequencyOfFailure, final int numberOfMachines, final int numberOfTables, final TableClustering clusteringSpec, final Set<WorkloadType> workloadSpecs) {

        final StringBuilder script = new StringBuilder();

        startMachines(numberOfMachines, script);

        createTables(numberOfTables, clusteringSpec, numberOfMachines, script);

        specifyWorkloadsToExecute(runtime, workloadSpecs, script);

        //TODO how do you specify the tables and workloads you want to create.

        return script.toString();
    }

    /**
     * Specify what workloads will run on what machines.
     * @param runtime How long the workloads should run for.
     * @param workloadSpec 
     * @param script
     */
    private static void specifyWorkloadsToExecute(final long runtime, final Set<WorkloadType> workloadSpecs, final StringBuilder script) {

        final int machineToRunWorkloadOn = 0; //TODO integrate with clustering spec, or provide other spec. mechanism?

        for (final WorkloadType workloadSpec : workloadSpecs) {
            final String workloadFileLocation = createWorkload(workloadSpec);
            script.append(SyntaxGenerator.executeWorkloadCommand(machineToRunWorkloadOn, workloadFileLocation, runtime));
        }

    }

    private static String createWorkload(final WorkloadType workloadSpec) {

        // TODO Auto-generated method stub
        return null;
    }

    public static void createTables(final int numberOfTables, final TableClustering clustering, final int numberOfMachines, final StringBuilder script) {

        int tableLocation = 0;

        for (int i = 0; i < numberOfTables; i++) { //creates all tables on the first machine.

            tableLocation = decideWhereToLocateTable(clustering, numberOfMachines, i, tableLocation);
            script.append(SyntaxGenerator.createTableCommand(tableLocation, SyntaxGenerator.sqlSimpleCreateTable(TABLE_NAME_PREFIX + nextTableID())));
        }
    }

    /**
     * Decide where to locate a new table based on the type of clustering (of tables) that is requested, and where tables have already been created.
     * @param clustering            Type of clustering to be employed.
     * @param numberOfMachines      The number of machines available.
     * @param tablesCreatedSoFar    The number of machines created so far.
     * @param lastTableLocation     The last machine to be assigned a table (indexed from 0, also initialized to 0).
     * @return
     */
    public static int decideWhereToLocateTable(final TableClustering clustering, final int numberOfMachines, final int tablesCreatedSoFar, int lastTableLocation) {

        int newTableLocation = 0;

        if (clustering.getClustering().equals(TableClustering.Clustering.COLOCATED)) {
            lastTableLocation = 0; //co-locate on the first machine.
        }
        else if (clustering.getClustering().equals(TableClustering.Clustering.GROUPED)) {
            if (tablesCreatedSoFar > 0 && tablesCreatedSoFar % clustering.getGroupSize() == 2) {
                newTableLocation = lastTableLocation + 1;

                if (newTableLocation > numberOfMachines - 1) {
                    newTableLocation = 0;
                }
            }
        }
        else if (clustering.getClustering().equals(TableClustering.Clustering.SPREAD)) {
            newTableLocation = tablesCreatedSoFar;

            if (newTableLocation > numberOfMachines - 1) {
                newTableLocation = 0;
            }
        }
        return newTableLocation;
    }

    public static void startMachines(final int numberOfMachines, final StringBuilder script) {

        for (int i = 0; i < numberOfMachines; i++) {
            script.append(createStartMachineCommand(nextMachineID(), i == 0));
        }
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
            startMachineCommand += "\n" + SyntaxGenerator.createSleepCommand(3000);
        }

        return startMachineCommand;
    }

    private static int nextMachineID() {

        return nextMachineID++;
    }

    private static int nextTableID() {

        return nextTableID++;
    }

}
