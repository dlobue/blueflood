package com.rackspacecloud.blueflood.io;

import java.util.List;

import com.rackspacecloud.blueflood.types.DiscoveryResult;
import com.rackspacecloud.blueflood.types.Metric;

public interface DiscoveryIO {
    public void insertDiscovery(List<Metric> metrics) throws Exception;
    public List<DiscoveryResult> getMetricsWithNameLike(String tenantId, String query);
    public List<DiscoveryResult> getMetrics(String tenantId);
    public DiscoveryResult getMetric(String tenantId, String metricName);
}
