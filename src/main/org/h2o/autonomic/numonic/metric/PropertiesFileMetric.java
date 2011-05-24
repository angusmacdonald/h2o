package org.h2o.autonomic.numonic.metric;

import java.io.IOException;

import org.h2o.util.H2OPropertiesWrapper;

public class PropertiesFileMetric implements IMetric {

    H2OPropertiesWrapper properties;

    public PropertiesFileMetric(final String propertiesFileLocation) throws IOException {

        properties = H2OPropertiesWrapper.getWrapper(propertiesFileLocation);
        properties.loadProperties();
    }

    @Override
    public double getCpuUtilization() {

        return Double.valueOf(properties.getProperty("cpu_utilization"));
    }

    @Override
    public double getMemoryUtilization() {

        return Double.valueOf(properties.getProperty("mem_utilization"));
    }

    @Override
    public double getSwapUtilization() {

        return Double.valueOf(properties.getProperty("swap_utilization"));
    }

    @Override
    public double getDiskUtilizationRead() {

        return Double.valueOf(properties.getProperty("disk_read_utilization"));
    }

    @Override
    public double getDiskUtilizationWrite() {

        return Double.valueOf(properties.getProperty("disk_write_utilization"));
    }

    @Override
    public double getNetworkUtilizationRead() {

        return Double.valueOf(properties.getProperty("network_read_utilization"));
    }

    @Override
    public double getNetworkUtilizationWrite() {

        return Double.valueOf(properties.getProperty("network_write_utilization"));
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
