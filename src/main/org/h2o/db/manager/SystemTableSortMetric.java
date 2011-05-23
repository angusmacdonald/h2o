package org.h2o.db.manager;

import org.h2o.autonomic.numonic.ranking.IMetric;

public class SystemTableSortMetric implements IMetric {

    //TODO a class is not the most appropriate structure for this. Properties file!!!
    private final double cpuUtilization = 1;
    private final double memoryUtilization = 1;
    private final double swapUtilization = 1;
    private final double diskUtilizationRead = 1;
    private final double diskUtilizationWrite = 1;
    private final double networkUtilizationRead = 1;
    private final double networkUtilizationWrite = 1;

    @Override
    public double getCpuUtilization() {

        return cpuUtilization;
    }

    @Override
    public double getMemoryUtilization() {

        return memoryUtilization;
    }

    @Override
    public double getSwapUtilization() {

        return swapUtilization;
    }

    @Override
    public double getDiskUtilizationRead() {

        return diskUtilizationRead;
    }

    @Override
    public double getDiskUtilizationWrite() {

        return diskUtilizationWrite;
    }

    @Override
    public double getNetworkUtilizationRead() {

        return networkUtilizationRead;
    }

    @Override
    public double getNetworkUtilizationWrite() {

        return networkUtilizationWrite;
    }

}
