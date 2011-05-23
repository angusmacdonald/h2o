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

}
