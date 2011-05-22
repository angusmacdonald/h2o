package org.h2o.autonomic.numonic.ranking;

import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;
import uk.ac.standrews.cs.numonic.sort.data.DistributionData;
import uk.ac.standrews.cs.numonic.sort.data.MachineInfo;
import uk.ac.standrews.cs.numonic.sort.data.SystemData;

/**
 * Responsible for ranking machines by a given sort metric.
 *
 * When this class is instantiated the sort metric must be specified. Then when the {@link #sortMachines(Set)} method is called the class
 * first iterates through each machine and stores the maximum CPU value and memory value for each - this is used to normalise the power of
 * each resource.
 *
 * <p>
 * The machines are then added to a sorted set, where the sort order is specified by the value assigned to each machine through
 * {@link MachineInfo#getValue(MachineSortingMetric, Bounds)}, which makes a call to
 * {@link #getMachineValue(MachineSortingMetric, SystemData, DistributionData, Bounds)} to obtain its value.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class MachineRanker implements Comparator<MachineMonitoringData> {

    /**
     * The metric used to sort machines.
     */
    IMetric metric;

    /**
     * The largest available CPU, memory and disk capacity. The values for this are set in {@link #getBoundsForNormalization(Set)}.
     */
    private final Bounds maxima = new Bounds();

    /**
     * Weight given to the size of swap space when calculating the amount of available memory.
     */
    private static final double SWAP_WEIGHT = 0.0;

    public MachineRanker(final IMetric sortingMetric) {

        metric = sortingMetric;
    }

    public SortedSet<MachineMonitoringData> sortMachines(final Set<MachineMonitoringData> machines) {

        getBoundsForNormalization(machines);

        final SortedSet<MachineMonitoringData> sorted_machines = new TreeSet<MachineMonitoringData>(this);
        sorted_machines.addAll(machines);

        return sorted_machines;
    }

    @Override
    public int compare(final MachineMonitoringData m1, final MachineMonitoringData m2) {

        final Double m1_value = getMachineValue(m1);
        final Double m2_value = getMachineValue(m2);

        return m1_value.compareTo(m2_value);
    }

    /**
     * Calculate the metric value of this machines availability, using CPU and memory information.
     * @param machine  Monitoring data on CPU and memory resources for a particular machine.
     * @return The metric value assigned to this machine based on its availability.
     */
    public double getMachineValue(final MachineMonitoringData machine) {

        final MachineUtilisationData machineData = machine.getMachineUtilData();

        double totalValue = 1;

        final double cpu_value = getCpuValue(machine);
        final double mem_value = getMemValue(machine);

        final double normalized_cpu_weight = normalize(cpu_value, maxima.getCpuMax(), 0);
        final double normalized_mem_weight = normalize(mem_value, maxima.getMemMax(), 0);

        // Value of resource = normalised power of resource * probability resource is available * importance of this resource to request.
        totalValue *= normalized_cpu_weight * machineData.getCpuUserTotal() * metric.getCpuUtilization();
        totalValue *= normalized_mem_weight * machineData.getMemoryUtilization() * metric.getMemoryUtilization();

        return totalValue;
    }

    /**
     * Returns a value representing the worth of this CPU based on its processing power, the number of cores, and the number of CPUs.
     * @return value used to weight distribution information when comparing machines.
     */
    private static double getCpuValue(final MachineMonitoringData machine) {

        final SystemInfoData staticMachineInfo = machine.getSystemInfoData();
        return getCpuValue(staticMachineInfo.getCpuClockSpeed(), staticMachineInfo.getNumberOfCores(), staticMachineInfo.getNumberOfCpus());
    }

    /**
     * Returns a value representing the capacity of this machines RAM. 
     * @return value used to weight distribution information when comparing machines.
     */
    private static double getMemValue(final MachineMonitoringData machine) {

        final SystemInfoData staticMachineInfo = machine.getSystemInfoData();
        return getMemValue(staticMachineInfo.getMemoryTotal(), staticMachineInfo.getSwapTotal());
    }

    /**
     * Returns a value representing the worth of this CPU based on its processing power, the number of cores, and the number of CPUs.
     * @return value used to weight distribution information when comparing machines.
     */
    private static double getCpuValue(final int cpu_mhz, final int num_cores, final int num_cpus) {

        return cpu_mhz * max(num_cpus, 1) * max(num_cores, 1);
    }

    /**
     * Returns a value representing the capacity of this machines RAM. 
     * @return value used to weight distribution information when comparing machines.
     */
    private static double getMemValue(final long memory_total, final long swap_total) {

        final double weight = memory_total + SWAP_WEIGHT * swap_total;

        return weight;
    }

    private static double normalize(final double value, final double max, final double min) {

        return (value - min) / (max - min);
    }

    /**
     * Go through every machine and find the maximum CPU, memory and disk capacity. This is used later to normalize data.
     * @param machines
     */
    public void getBoundsForNormalization(final Set<MachineMonitoringData> machines) {

        for (final MachineMonitoringData machine : machines) {
            maxima.setCpuMax(max(getCpuValue(machine), maxima.getCpuMax()));
            maxima.setMemMax(max(getMemValue(machine), maxima.getMemMax()));
        }
    }

    /**
     * Returns the maximum of two values.
     * @param a the first value.
     * @param b the second value.
     * @return the maximum.
     */
    private static int max(final int a, final int b) {

        return a > b ? a : b;
    }

    /**
     * Returns the maximum of two values.
     * @param a the first value.
     * @param b the second value.
     * @return the maximum.
     */
    private static double max(final double a, final double b) {

        return a > b ? a : b;
    }
}