package org.h2o.autonomic.numonic;

import org.h2o.autonomic.numonic.ranking.IMetric;

public class Metric implements IMetric {

    private final double cpu;
    private final double memory;
    private final double swap;
    private final double disk_r;
    private final double disk_w;
    private final double network_r;
    private final double network_w;

    public Metric(final double cpu, final double memory, final double swap, final double disk_r, final double disk_w, final double network_r, final double network_w) {

        this.cpu = cpu;
        this.memory = memory;
        this.swap = swap;
        this.disk_r = disk_r;
        this.disk_w = disk_w;
        this.network_r = network_r;
        this.network_w = network_w;

    }

    @Override
    public double getCpuUtilization() {

        return cpu;
    }

    @Override
    public double getMemoryUtilization() {

        return memory;
    }

    @Override
    public double getSwapUtilization() {

        return swap;
    }

    @Override
    public double getDiskUtilizationRead() {

        return disk_r;
    }

    @Override
    public double getDiskUtilizationWrite() {

        return disk_w;
    }

    @Override
    public double getNetworkUtilizationRead() {

        return network_r;
    }

    @Override
    public double getNetworkUtilizationWrite() {

        return network_w;
    }

}
