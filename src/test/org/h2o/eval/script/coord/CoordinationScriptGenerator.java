package org.h2o.eval.script.coord;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableGrouping;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.workload.WorkloadGenerator;

public class CoordinationScriptGenerator {

    private static final String TABLE_NAME_PREFIX = "test";

    private static int nextMachineID = 0;
    private static int nextTableID = 0;

    public static String generateCoordinationScript(final long runtime, final double probabilityOfFailure, final double frequencyOfFailure, final int numberOfMachines, final int numberOfTables, final TableClustering clusteringSpec, final Set<WorkloadType> workloadSpecs) throws IOException {

        final StringBuilder script = new StringBuilder();

        startMachines(numberOfMachines, script);

        final TableGrouping tableGrouping = createTables(numberOfTables, clusteringSpec, numberOfMachines, script);

        specifyWorkloadsToExecute(runtime, workloadSpecs, tableGrouping, script);

        return script.toString();
    }

    /**
     * Specify what workloads will run on what machines.
     * @param runtime How long the workloads should run for.
     * @param tableGrouping 
     * @param workloadSpec 
     * @param script
     * @throws IOException Couldn't create folder to store workload files.
     */
    private static void specifyWorkloadsToExecute(final long runtime, final Set<WorkloadType> workloadSpecs, final TableGrouping tableGrouping, final StringBuilder script) throws IOException {

        for (final WorkloadType workloadSpec : workloadSpecs) {
            /*
             * This has to pass in all of the tables involved in the particular workload, so if it passes in all of tableNames
             * it will include queries for each table.
             */
            Map<String, Integer> workloadFileLocations = null;

            workloadFileLocations = new WorkloadGenerator().createWorkloads(workloadSpec, tableGrouping);

            for (final Entry<String, Integer> workloadEntry : workloadFileLocations.entrySet()) {

                script.append(SyntaxGenerator.executeWorkloadCommand(workloadEntry.getValue(), workloadEntry.getKey(), runtime));

            }
        }

    }

    /**
     * 
     * @param numberOfTables
     * @param clustering
     * @param numberOfMachines
     * @param script
     * @return {@link TableGrouping} object which specifies how tables are located/grouped together.
     */
    public static TableGrouping createTables(final int numberOfTables, final TableClustering clustering, final int numberOfMachines, final StringBuilder script) {

        final TableGrouping grouping = new TableGrouping();

        int tableLocation = 0;

        for (int i = 0; i < numberOfTables; i++) { //creates all tables on the first machine.

            tableLocation = decideWhereToLocateTable(clustering, numberOfMachines, i, tableLocation);
            final String tableName = TABLE_NAME_PREFIX + nextTableID();

            grouping.addTable(tableLocation, tableName);
            script.append(SyntaxGenerator.createTableCommand(tableLocation, SyntaxGenerator.sqlSimpleCreateTable(tableName)));
        }

        return grouping;
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
