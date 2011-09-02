package org.h2o.eval;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.h2o.eval.script.coord.CoordinationScriptGenerator;
import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableClustering.Clustering;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.coord.specification.WorkloadType.LinkToTableLocation;

/**
 * Generates a co-ordination script based on the variables in the main method of this class. Returns the location of the co-ordination script file that it creates.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class ScriptGenerator {

    public static void main(final String[] args) throws IOException {

        final long runtime = 480000; //eight minutes.
        final double probabilityOfFailure = 0.05;
        final long frequencyOfFailure = 60000;
        final int numberOfMachines = 6;
        final int numberOfTables = 1;
        final TableClustering clusteringSpec = new TableClustering(Clustering.GROUPED, 5);

        final Set<WorkloadType> workloadSpecs = new HashSet<WorkloadType>();
        final WorkloadType spec = new WorkloadType(0.5, false, 0, true, 50, LinkToTableLocation.WORKLOAD_PER_TABLE, false);
        workloadSpecs.add(spec);

        final String scriptLocation = CoordinationScriptGenerator.generateCoordinationScript(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, clusteringSpec, workloadSpecs);

        System.out.println("Co-ordination script saved to: " + scriptLocation);
    }
}
