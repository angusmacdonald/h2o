package org.h2o.eval.script.coord;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableGrouping;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.workload.WorkloadGenerator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import uk.ac.standrews.cs.nds.util.FileUtil;

public class CoordinationScriptGenerator {

    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    private static final String TABLE_NAME_PREFIX = "test";

    private static int nextMachineID = 0;
    private static int nextTableID = 0;

    /**
     * Generate a co-ordinator script based on the following parameters.
     * @param runtime How long the script should execute for (in milliseconds)
     * @param probabilityOfFailure The likelihood of failure at a given point in time (% chance where 1 is 100% and 0.1 is 10%...).
     * @param frequencyOfFailure How often the system should decide whether to fail a node (or nodes) in milliseconds. E.g. if set to 1000, the system will use #probabilityOfFailure every 1 second to determine whether to fail each node.
     * @param numberOfMachines The number of machines to be started in this system.
     * @param numberOfTables The number of tables to be created.
     * @param clusteringSpec How tables should be spread over machines.
     * @param workloadSpecs How many workloads should be created, and what types of workloads should be created.
     * @return Absolute path to the co-ordination script file that has been generated by this process.
     * @throws IOException
     */
    public static String generateCoordinationScript(final long runtime, final double probabilityOfFailure, final double frequencyOfFailure, final int numberOfMachines, final int numberOfTables, final TableClustering clusteringSpec, final Set<WorkloadType> workloadSpecs) throws IOException {

        final String folderPath = "generatedWorkloads" + File.separator + dateFormatter.format(System.currentTimeMillis());

        final StringBuilder script = new StringBuilder();

        startMachines(numberOfMachines, script);

        final TableGrouping tableGrouping = createTables(numberOfTables, clusteringSpec, numberOfMachines, script);

        specifyWorkloadsToExecute(runtime, workloadSpecs, tableGrouping, script, folderPath);

        final String scriptLocation = folderPath + File.separator + "coordinator.coord";
        FileUtil.writeToFile(scriptLocation, script.toString());

        return scriptLocation;
    }

    /**
     * Specify what workloads will run on what machines.
     * @param runtime How long the workloads should run for.
     * @param tableGrouping 
     * @param workloadSpec 
     * @param script
     * @param folderPath 
     * @throws IOException Couldn't create folder to store workload files.
     */
    private static void specifyWorkloadsToExecute(final long runtime, final Set<WorkloadType> workloadSpecs, final TableGrouping tableGrouping, final StringBuilder script, final String folderPath) throws IOException {

        for (final WorkloadType workloadSpec : workloadSpecs) {
            /*
             * This has to pass in all of the tables involved in the particular workload, so if it passes in all of tableNames
             * it will include queries for each table.
             */
            Map<String, Integer> workloadFileLocations = null;

            workloadFileLocations = new WorkloadGenerator().createWorkloads(workloadSpec, tableGrouping, folderPath);

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
    public static int decideWhereToLocateTable(final TableClustering clustering, final int numberOfMachines, final int tablesCreatedSoFar, final int lastTableLocation) {

        int newTableLocation = -1;

        if (clustering.getClustering().equals(TableClustering.Clustering.COLOCATED)) {
            newTableLocation = 0; //co-locate on the first machine.
        }
        else if (clustering.getClustering().equals(TableClustering.Clustering.GROUPED)) {
            if (tablesCreatedSoFar > 0 && tablesCreatedSoFar % clustering.getGroupSize() == 0) {
                newTableLocation = lastTableLocation + 1;

                if (newTableLocation > numberOfMachines - 1) {
                    newTableLocation = 0;
                }
            }
            else {
                newTableLocation = lastTableLocation;
            }
        }
        else if (clustering.getClustering().equals(TableClustering.Clustering.SPREAD)) {
            newTableLocation = tablesCreatedSoFar;

            if (newTableLocation > numberOfMachines - 1) {
                newTableLocation = 0;
            }
        }
        else {
            throw new NotImplementedException();
        }

        return newTableLocation;
    }

    public static void startMachines(final int numberOfMachines, final StringBuilder script) {

        for (int i = 0; i < numberOfMachines; i++) {
            script.append(SyntaxGenerator.createStartMachineCommand(nextMachineID(), i == 0));
        }
    }

    private static int nextMachineID() {

        return nextMachineID++;
    }

    private static int nextTableID() {

        return nextTableID++;
    }

}
