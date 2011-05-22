package org.h2o.autonomic.numonic.ranking;

/**
 * The maximum values given to each resource across all of the machines being compared.
 * This data is used to normalise the values of resources when taking them into account in ranking machines.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class Bounds {

    private double cpu_max;
    private double mem_max;

    /**
     * Returns the maximum CPU value.
     * @return the maximum CPU value
     */
    public double getCpuMax() {

        return cpu_max;
    }

    /**
     * Returns the maximum memory value.
     * @return the maximum memory value
     */
    public double getMemMax() {

        return mem_max;
    }

    /**
     * Sets the maximum CPU value.
     * @param cpu_max the maximum CPU value
     */
    public void setCpuMax(final double cpu_max) {

        this.cpu_max = cpu_max;
    }

    /**
     * Sets the maximum memory value.
     * @param mem_max the maximum memory value
     */
    public void setMemMax(final double mem_max) {

        this.mem_max = mem_max;
    }
}
