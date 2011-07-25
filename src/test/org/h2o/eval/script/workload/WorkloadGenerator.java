package org.h2o.eval.script.workload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.h2o.eval.script.coord.specification.TableGrouping;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.coord.specification.WorkloadType.LinkToTableLocation;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import uk.ac.standrews.cs.nds.util.FileUtil;

public class WorkloadGenerator {

    private static final int QUERIES_IN_SCRIPT = 100;

    private File workloadFolder;

    /**
     * The number of workload files that have been created so far. Used to name the files.
     */
    private int fileCount = 0;

    /**
     * Creates a set of workloads (the precise number determined by the configuration specified in the following parameters).
     * @param spec Workload configuration.
     * @return Location of the newly generated workload file and the machine it is to be run on.
     * @throws IOException 
     */
    public Map<String, Integer> createWorkloads(final WorkloadType spec, final TableGrouping tableGrouping, final String folderPath) throws IOException {

        createWorkloadsFolder(folderPath);

        final Map<String, Integer> newWorkloads = new HashMap<String, Integer>();

        final Map<Integer, ArrayList<String>> groupings = tableGrouping.getGroupings();

        if (spec.getLinkToTableLocation().equals(LinkToTableLocation.ALL_ENCOMPASSING_WORKLOAD)) {
            //A single workload is created which queries every table.
            final int locationToRunWorkload = 0;

            final ArrayList<String> tablesInWorkload = new ArrayList<String>();

            for (final List<String> tablesInGroup : groupings.values()) {
                tablesInWorkload.addAll(tablesInGroup);
            }

            final String workloadLocation = createWorkload(spec, tablesInWorkload, "allTables.workload");

            newWorkloads.put(workloadLocation, locationToRunWorkload);
        }
        else if (spec.getLinkToTableLocation().equals(LinkToTableLocation.WORKLOAD_PER_TABLE)) {
            //One workload is created per table.

            for (final Entry<Integer, ArrayList<String>> groupEntry : groupings.entrySet()) { //Loop through each group

                for (final String table : groupEntry.getValue()) { //Loop through individual tables in this group

                    //Create an array list with just a single element so that it can be passed to the createWorkload call.
                    final ArrayList<String> singleTable = new ArrayList<String>();
                    singleTable.add(table);
                    final String workloadLocation = createWorkload(spec, singleTable, "table-" + table + ".workload");

                    addToCreatedWorkloadsAtLocation(spec.isQueriesLocalToTables(), tableGrouping.getTotalNumberOfMachines(), newWorkloads, groupEntry, workloadLocation);
                }

            }

        }
        else if (spec.getLinkToTableLocation().equals(LinkToTableLocation.GROUPED_WORKLOAD)) {
            for (final Entry<Integer, ArrayList<String>> groupEntry : groupings.entrySet()) { //Loop through each group

                final String workloadLocation = createWorkload(spec, groupEntry.getValue(), "group" + fileCount++ + ".workload");

                addToCreatedWorkloadsAtLocation(spec.isQueriesLocalToTables(), tableGrouping.getTotalNumberOfMachines(), newWorkloads, groupEntry, workloadLocation);

            }
        }
        else {
            throw new NotImplementedException();
        }

        return newWorkloads;
    }

    /**
     * Adds the location of the newly created workload file to the newWorkloads hashmap. Where the workload is set to be run is determined by
     * the localToTable parameter. If true, it will be run on the same machine as a table; if false, it will be run on another machine.
     * 
     */
    private void addToCreatedWorkloadsAtLocation(final boolean localToTable, final int totalNumberOfMachines, final Map<String, Integer> newWorkloads, final Entry<Integer, ArrayList<String>> groupEntry, final String workloadLocation) {

        final Integer machineLocation = groupEntry.getKey();

        //Decide whether to run the workload local to a table, or remote from it.
        if (localToTable) {
            newWorkloads.put(workloadLocation, machineLocation);
        }
        else {

            final Integer remoteLocation = machineLocation + 1 >= totalNumberOfMachines ? 0 : machineLocation + 1;
            newWorkloads.put(workloadLocation, remoteLocation);
        }
    }

    /**
     * Create the folder in which workload files will be saved.
     * @throws IOException If the folder could not be created.
     */
    private void createWorkloadsFolder(final String folderPath) throws IOException {

        workloadFolder = new File(folderPath);
        final boolean successful = workloadFolder.mkdirs();

        if (!successful) { throw new IOException("Failed to create folders in which to store workloads."); }
    }

    /**
     * Create a new workload based on the provided configuration, and return the location of the workload file created as a result.
     * @param spec 
     * @param tablesInWorkload
     * @return location of newly created workload file.
     * @throws FileNotFoundException Thrown if it wasn't possible to create the workload file.
     */
    private String createWorkload(final WorkloadType spec, final ArrayList<String> tablesInWorkload, final String fileName) throws FileNotFoundException {

        final String workloadFileLocation = workloadFolder.getAbsolutePath() + File.separator + fileName;

        if (spec.isQueryAgainstSystemTable()) {
            throw new NotImplementedException();
        }
        else {

            final StringBuilder script = createTableManagerWorkload(spec, tablesInWorkload);

            FileUtil.writeToFile(workloadFileLocation, script.toString());

        }

        return workloadFileLocation;

    }

    private StringBuilder createTableManagerWorkload(final WorkloadType spec, final ArrayList<String> tablesInWorkload) {

        final StringBuilder script = new StringBuilder();

        final Random r = new Random(System.currentTimeMillis());

        final boolean autoCommitOff = spec.isMultiQueryTransactionsEnabled();

        if (autoCommitOff) {
            script.append("SET AUTOCOMMIT OFF;\n");
        }

        for (int i = 0; i < Math.max(spec.getQueriesPerTransaction() * 10, QUERIES_IN_SCRIPT); i++) {
            /*
             * In this loop i is used to determine which table is to be queried next ('i % tablesInWorkload.size()') and
             * when the next commit statement needs to be added ('i % spec.getQueriesPerTransaction() == 0')
             */

            if (r.nextDouble() > spec.getReadWriteRatio()) { //this should be a write.
                script.append("INSERT INTO " + tablesInWorkload.get(i % tablesInWorkload.size()) + " VALUES (<loop-counter/>, <generated-string/>, <generated-string/>, <generated-string/>, <generated-string/>, <generated-long/>, <generated-long/>);\n");
            }
            else { //this should be a read.
                script.append("SELECT * FROM " + tablesInWorkload.get(i % tablesInWorkload.size()) + " WHERE int_a > " + WorkloadExecutor.generateBigIntegerValue() + ";\n");
            }

            if (spec.getSleepTime() > 0) {
                script.append("<sleep>" + spec.getSleepTime() + "</sleep>\n");
            }

            if (autoCommitOff && i % spec.getQueriesPerTransaction() == 0 && i > 0) {
                script.append("COMMIT;\n");
            }
        }

        if (autoCommitOff) {
            script.append("SET AUTOCOMMIT ON;\n");
        }
        return script;
    }
}
