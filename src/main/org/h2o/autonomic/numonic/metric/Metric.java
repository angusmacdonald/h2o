package org.h2o.autonomic.numonic.metric;

public class Metric implements IMetric {

    protected double cpu;
    protected double memory;
    protected double swap;
    protected double disk_r;
    protected double disk_w;
    protected double network_r;
    protected double network_w;

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

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(cpu);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(disk_r);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(disk_w);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(memory);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(network_r);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(network_w);
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(swap);
        result = prime * result + (int) (temp ^ temp >>> 32);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final Metric other = (Metric) obj;
        if (Double.doubleToLongBits(cpu) != Double.doubleToLongBits(other.cpu)) { return false; }
        if (Double.doubleToLongBits(disk_r) != Double.doubleToLongBits(other.disk_r)) { return false; }
        if (Double.doubleToLongBits(disk_w) != Double.doubleToLongBits(other.disk_w)) { return false; }
        if (Double.doubleToLongBits(memory) != Double.doubleToLongBits(other.memory)) { return false; }
        if (Double.doubleToLongBits(network_r) != Double.doubleToLongBits(other.network_r)) { return false; }
        if (Double.doubleToLongBits(network_w) != Double.doubleToLongBits(other.network_w)) { return false; }
        if (Double.doubleToLongBits(swap) != Double.doubleToLongBits(other.swap)) { return false; }
        return true;
    }

}
