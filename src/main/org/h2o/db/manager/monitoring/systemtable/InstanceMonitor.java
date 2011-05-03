package org.h2o.db.manager.monitoring.systemtable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.h2o.autonomic.decision.ranker.metric.Metric;
import org.h2o.autonomic.decision.ranker.metric.MetricEvaluatior;
import org.h2o.autonomic.framework.MonitoringData;

import uk.ac.standrews.cs.numonic.appinterface.ResourceType;

public class InstanceMonitor implements IMachineRanking {

    /**
     * Summaries of resource information from each database instance.
     */
    List<InstanceResourceSummary> summaries;
    private final Metric metric;

    public InstanceMonitor(final Metric metric) {

        this.metric = metric;
        summaries = new LinkedList<InstanceResourceSummary>();
    }

    @Override
    public void addMonitoringSummary(final InstanceResourceSummary summary) {

        summaries.add(summary);
    }

    @Override
    public SortedSet<InstanceResourceSummary> getRankedListOfInstances() {

        //Rank Machines.

        final Map<ResourceType, double[]> resultsStore = new HashMap<ResourceType, double[]>(); //stores the normalized results of each resource.

        for (final ResourceType resourceType : metric.getResourceImportanceMap().keySet()) { //for each resource that this metric talks about.

            final double[] data = collateAllMeasurementsForResource(resourceType);

            final double[] normalizedResults = MetricEvaluatior.normalizeResults(data);

            resultsStore.put(resourceType, normalizedResults); //key: resource type, value: array of normalized results - each entry representing a database instance.
        }

        final List<InstanceResourceSummary> normalizedResourceSummary = collateNormalizedResults(resultsStore);

        return produceRanking(metric, normalizedResourceSummary);
    }

    /**
     * The normalized results in the resultsStore parameter are stored in a seperate array for each resource. This method flips the storage pattern
     * so that normalized results are stored in a separate object for each instance.
     * @param resultsStore
     * @return
     */
    private List<InstanceResourceSummary> collateNormalizedResults(final Map<ResourceType, double[]> resultsStore) {

        final List<InstanceResourceSummary> normalizedResourceSummary = new LinkedList<InstanceResourceSummary>();

        for (int i = 0; i < summaries.size(); i++) {
            final InstanceResourceSummary originalSummary = summaries.get(i);

            final List<MonitoringData> normalizedSamples = new LinkedList<MonitoringData>();

            for (final Entry<ResourceType, double[]> resourceEntry : resultsStore.entrySet()) {
                //Create new monitoring data object. Take location from existing object. Take everything else from normalized value field.
            }

            final InstanceResourceSummary normalizedSummary = new InstanceResourceSummary(originalSummary.getDatabaseID(), originalSummary.getMonitoringStartTime(), originalSummary.getMonitoringEndTime(), originalSummary.getNumberOfSamples(), normalizedSamples);

            normalizedResourceSummary.add(normalizedSummary);
        }

        return normalizedResourceSummary;
    }

    private SortedSet<InstanceResourceSummary> produceRanking(final Metric metric, final List<InstanceResourceSummary> normalizedResourceSummary) {

        return null;

        // TODO Auto-generated method stub

    }

    /**
     * Get all data taken for a specific resource. Each entry in this array represents monitoring data from a different database instane.
     * @param resourceType The resource to get information for.
     * @return
     */
    public double[] collateAllMeasurementsForResource(final ResourceType resourceType) {

        final double[] data = new double[summaries.size()];

        for (int i = 0; i < summaries.size(); i++) { //for each machine, extract the data for this resource.
            final InstanceResourceSummary instanceSummary = summaries.get(i);

            final MonitoringData monitoringObject = instanceSummary.get(resourceType);

            if (monitoringObject != null) {
                data[i] = (Double) monitoringObject.getData();
            }
        }

        return data;
    }

}
