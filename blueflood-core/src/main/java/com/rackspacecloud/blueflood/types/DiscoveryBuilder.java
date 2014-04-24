/**
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.types;

import java.io.Serializable;

public class DiscoveryBuilder implements Serializable {
    private String metricName;
    private String tenantId;
    private String unit;
    private String type;

    public DiscoveryBuilder(String tenantId) {
        this.tenantId = tenantId;
    }

    public DiscoveryBuilder(String tenantId, String metricName) {
        this.tenantId = tenantId;
        this.metricName = metricName;
    }

    public DiscoveryBuilder setMetricName(String metricName) {
        this.metricName = metricName;
        return this;
    }

    public DiscoveryBuilder setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    public DiscoveryBuilder setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public DiscoveryBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public String getUnit() {
        return unit;
    }

    public String getType() {
        return type;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getDocumentId() {
        return tenantId + ":" + metricName;
    }
}
