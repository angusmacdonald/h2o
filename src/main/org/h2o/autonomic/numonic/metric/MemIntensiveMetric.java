package org.h2o.autonomic.numonic.metric;

public class MemIntensiveMetric implements IMetric {

    private final double cpu = 0.2;
    private final double memory = 1;
    private final double swap = 0;
    private final double disk_r = 0;
    private final double disk_w = 0;
    private final double network_r = 0;
    private final double network_w = 0;

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
        temp = Double.doubleToLongBits(getCpuUtilization());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getDiskUtilizationRead());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getDiskUtilizationWrite());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getMemoryUtilization());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getMemoryUtilization());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getNetworkUtilizationWrite());
        result = prime * result + (int) (temp ^ temp >>> 32);
        temp = Double.doubleToLongBits(getSwapUtilization());
        result = prime * result + (int) (temp ^ temp >>> 32);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final Metric other = (Metric) obj;
        if (Double.doubleToLongBits(getCpuUtilization()) != Double.doubleToLongBits(other.getCpuUtilization())) { return false; }
        if (Double.doubleToLongBits(getDiskUtilizationRead()) != Double.doubleToLongBits(other.getDiskUtilizationRead())) { return false; }
        if (Double.doubleToLongBits(getDiskUtilizationWrite()) != Double.doubleToLongBits(other.getDiskUtilizationWrite())) { return false; }
        if (Double.doubleToLongBits(getMemoryUtilization()) != Double.doubleToLongBits(other.getMemoryUtilization())) { return false; }
        if (Double.doubleToLongBits(getNetworkUtilizationRead()) != Double.doubleToLongBits(other.getNetworkUtilizationRead())) { return false; }
        if (Double.doubleToLongBits(getNetworkUtilizationWrite()) != Double.doubleToLongBits(other.getNetworkUtilizationWrite())) { return false; }
        if (Double.doubleToLongBits(getSwapUtilization()) != Double.doubleToLongBits(other.getSwapUtilization())) { return false; }
        return true;
    }
}
