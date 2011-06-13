package org.h2o.monitoring;

import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;

public class ResourceSpec {

    public final String databaseName;

    public final int cpuClockSpeed;
    public final int numCores;
    public final int numCPUs;
    public double cpuUtilization;

    public final int memoryCapacity;
    public final double memoryUtilization;

    public final int swapCapacity;
    public final double swapUtilization;

    public final double utilizationOfDisk;
    public final int diskCapacityGB;

    public final int measurements_before_summary;

    /**
     * @param databaseName 
     * @param cpuClockSpeed
     * @param numCores
     * @param numCPUs
     * @param memoryCapacity
     * @param memoryUtilization
     * @param swapCapacity
     * @param swapUtilization
     * @param utilizationOfDisk
     * @param diskCapacityGB
     * @param measurements_before_summary
     */
    public ResourceSpec(final String databaseName, final double cpuUtilization, final int cpuClockSpeed, final int numCores, final int numCPUs, final int memoryCapacity, final double memoryUtilization, final int swapCapacity, final double swapUtilization, final double utilizationOfDisk,
                    final int diskCapacityGB, final int measurements_before_summary) {

        this.databaseName = databaseName;
        this.cpuClockSpeed = cpuClockSpeed;
        this.numCores = numCores;
        this.numCPUs = numCPUs;
        this.cpuUtilization = cpuUtilization;
        this.memoryCapacity = memoryCapacity;
        this.memoryUtilization = memoryUtilization;
        this.swapCapacity = swapCapacity;
        this.swapUtilization = swapUtilization;
        this.utilizationOfDisk = utilizationOfDisk;
        this.diskCapacityGB = diskCapacityGB;
        this.measurements_before_summary = measurements_before_summary;
    }

    /**
     * Creates synthetic resource data based on some general configuration parameters. The parameters beginning 'powerOf' are used to scale the synthetic data. If the value '10' is used
     * for each of these, a machine with the following spec. will be generated (the value 10 is a multiplier): 8000Mhz CPU, 8Gb Mem, 8Gb Swap, 1Tb Disk.
     * 
     *  <p> The CPU capacity is 8000MHz (at powerOfCPU=10) because it assumes this is a single core machine - but in metric calculations the number of cores and CPUs is used as a multiplier
     *  on the power of the machine, so 8000MHz represents a typical machine.
     * @param string 
     * @param powerOfCPU        Multiplier for the relative power of this machine's CPU (10=8000MHz).
     * @param powerOfMemory     Multiplier for the relative size of this machine's memory+swap (10=8Gb). 
     * @param powerOfDisk       Multiplier for the relative size of this machine's disk (10=1Tb).
     * @param cpuUtil           % utilization of CPU. 
     * @param memUtil           % utilization of memory.
     * @param diskUtil          % utilization of disk.
     * @return a new object containing enough synthetic data to create more elaborate synthetic monitoring results.
     */
    public static MachineMonitoringData generateMonitoringData(final String dbName, final int powerOfCPU, final int powerOfMemory, final int powerOfDisk, final double cpuUtil, final double memUtil, final double diskUtil) {

        final MachineMonitoringData monitoringData = generateMonitoringData(generateResourceSpec(dbName, powerOfCPU, powerOfMemory, powerOfDisk, cpuUtil, memUtil, diskUtil));
        monitoringData.setDatabaseWrapper(new DatabaseInstanceWrapper(DatabaseID.parseURL(dbName), null, true));

        return monitoringData;
    }

    private static ResourceSpec generateResourceSpec(final String dbName, final int powerOfCPU, final int powerOfMemory, final int powerOfDisk, final double cpuUtil, final double memUtil, final double diskUtil) {

        return new ResourceSpec(dbName, cpuUtil, 800 * powerOfCPU, 1, 1, 8000 * powerOfMemory, memUtil, 8000 * powerOfMemory, memUtil / 2, diskUtil, 100 * powerOfDisk, 10);

    }

    public static MachineMonitoringData generateMonitoringData(final ResourceSpec spec) {

        final SystemInfoData staticSysInfoData = generateSysInfoData(spec.databaseName, spec.numCores, spec.numCPUs, spec.cpuClockSpeed, spec.memoryCapacity, spec.swapCapacity);

        final MachineUtilisationData machineUtilData = generateMachineUtilData(spec.cpuUtilization, (long) (spec.memoryCapacity * spec.memoryUtilization), (long) (spec.memoryCapacity * (1 - spec.memoryUtilization)), (long) (spec.swapCapacity * spec.swapUtilization),
                        (long) (spec.swapCapacity * (1 - spec.swapUtilization)));

        final FileSystemData fsData = generateFileSystemData(spec.utilizationOfDisk, spec.diskCapacityGB * 1024 * 1024);

        final DatabaseID localDatabaseID = DatabaseID.parseURL(spec.databaseName);

        return new MachineMonitoringData(localDatabaseID, staticSysInfoData, machineUtilData, fsData, spec.measurements_before_summary);
    }

    private static FileSystemData generateFileSystemData(final double utilizationOfDisk, final long diskCapacity) {

        final FileSystemData fsData = new FileSystemData();
        fsData.file_system_location = "C:\\";
        fsData.file_system_name = "C";
        fsData.file_system_type = "local";

        fsData.fs_size = diskCapacity;
        fsData.fs_space_free = (long) (fsData.fs_size * (1 - utilizationOfDisk));
        fsData.fs_space_used = (long) (fsData.fs_size * utilizationOfDisk);

        return fsData;
    }

    private static MachineUtilisationData generateMachineUtilData(final double utilizationOfCPU, final long memory_used, final long memory_free, final long swap_used, final long swap_free) {

        final double cpu_user_total = utilizationOfCPU;
        final double cpu_sys_total = 0; //not used in ranking.
        final double cpu_idle_total = 1 - utilizationOfCPU;
        final double cpu_wait_total = 0;//not used in ranking.
        final double cpu_nice_total = 0;//not used in ranking.

        return new MachineUtilisationData(cpu_user_total, cpu_sys_total, cpu_idle_total, cpu_wait_total, cpu_nice_total, memory_used, memory_free, swap_used, swap_free);

    }

    private static SystemInfoData generateSysInfoData(final String dbName, final int numCores, final int numCPUs, final int cpuClockSpeed, final int memoryCapacity, final int swapCapacity) {

        final int number_of_cores = numCores;
        final int number_of_cpus = numCPUs;
        final int cpu_clock_speed = cpuClockSpeed;
        final long memory_total = memoryCapacity;
        final long swap_total = swapCapacity;

        return new SystemInfoData(dbName, "SimOS", "7", "localhost", "127.0.0.1", "255.255.255.255", "SimVendor", "SimModel", number_of_cores, number_of_cpus, cpu_clock_speed, -1, memory_total, swap_total, "true");

    }

}
