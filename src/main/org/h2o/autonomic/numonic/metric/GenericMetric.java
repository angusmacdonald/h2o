package org.h2o.autonomic.numonic.metric;

public class GenericMetric extends Metric implements IMetric {

    public GenericMetric(final double cpu, final double memory, final double swap, final double disk_r, final double disk_w, final double network_r, final double network_w) {

        super(cpu, memory, swap, disk_r, disk_w, network_r, network_w);

    }

}
