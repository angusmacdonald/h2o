package org.h2o.autonomic.numonic.ranking;

/**
 * Used to filter out machines that don't meet the requirements of a particular request.
 *
 * <p>
 * Instances of this class specify the minimum amount of resources needed for a request.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class Requirements {

    private static final int BITS_SHIFTED = 32;

    /**
     * Default requirements which will not filter any machines.
     */
    public static final Requirements NO_FILTERING = new Requirements(0, 0, 0, 0);

    /**
     * Doesn't include instances that have been set as 'NO_REPLICATE', meaning they shouldn't receive any replicas.
     */
    public static final Requirements FILTER_NO_REPLICA_INSTANCES = new Requirements(2, 2, 2, 2);

    /**
     * The amount of CPU capacity required, in MHz.
     */
    private final long cpu_capacity;

    /**
     * The quantity of memory required, in KB.
     */
    private final long memory_capacity;

    /**
     * The quantity of disk space required, in KB.
     */
    private final long disk_capacity;

    /**
     * The bandwidth of network connection required, in KB/s.
     */
    private final long network_capacity;

    // -------------------------------------------------------------------------------------------------------

    /**
     * Initializes a set of requirements.
     *
     * @param cpu_capacity the amount of CPU capacity required, in MHz
     * @param memoryCapacity the quantity of memory required, in Kb
     * @param diskCapacity the quantity of disk space required, in Kb
     * @param network_capacity the bandwidth of network connection required, in KB/s
     */
    public Requirements(final long cpu_capacity, final long memoryCapacity, final long diskCapacity, final long network_capacity) {

        this.cpu_capacity = cpu_capacity;
        memory_capacity = memoryCapacity;
        disk_capacity = diskCapacity;
        this.network_capacity = network_capacity;
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Returns the amount of CPU capacity required.
     * @return the amount of CPU capacity required, in MHz
     */
    public long getCpuCapacity() {

        return cpu_capacity;
    }

    /**
     * Returns the quantity of memory required.
     * @return the quantity of memory required, in KB
     */
    public long getMemoryCapacity() {

        return memory_capacity;
    }

    /**
     * Returns the quantity of disk space required.
     * @return the quantity of disk space required, in KB
     */
    public long getDiskCapacity() {

        return disk_capacity;
    }

    /**
     * Returns the bandwidth of network connection required.
     * @return the bandwidth of network connection required, in KB/s
     */
    public long getNetworkCapacity() {

        return network_capacity;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (cpu_capacity ^ cpu_capacity >>> BITS_SHIFTED);
        result = prime * result + (int) (disk_capacity ^ disk_capacity >>> BITS_SHIFTED);
        result = prime * result + (int) (memory_capacity ^ memory_capacity >>> BITS_SHIFTED);
        result = prime * result + (int) (network_capacity ^ network_capacity >>> BITS_SHIFTED);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (!(obj instanceof Requirements)) { return false; }

        final Requirements other = (Requirements) obj;

        return cpu_capacity == other.cpu_capacity && disk_capacity == other.disk_capacity && memory_capacity == other.memory_capacity && network_capacity == other.network_capacity;
    }
}
