/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.DiscoveryBuilder;
import com.rackspacecloud.blueflood.types.DiscoveryResult;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.Util;

import com.codahale.metrics.Timer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import static com.rackspacecloud.blueflood.io.ElasticIO.ESFieldLabel.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ElasticIO implements DiscoveryIO {
//    static enum ESFieldLabel {
//        METRIC_NAME,
//        TENANT_ID,
//        TYPE,
//        UNIT
//    }

    private static final Logger log = LoggerFactory.getLogger(DiscoveryIO.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Client client;
    private static final String ES_TYPE = "metrics";
    private static final String METRIC_NAME = "metricName";
    private static final String TYPE = "type";
    private static final String TENANT_ID = "tenantId";
    private static final String UNIT = "unit";
    private static final String INDEX_PREFIX = "blueflood-";
    private final Timer searchTimer = Metrics.timer(ElasticIO.class, "Search Duration");
    private final Timer getmetadataTimer = Metrics.timer(ElasticIO.class, "Get Metadata Timer");

    public static String getIndexPrefix() {
        return INDEX_PREFIX;
    }

    public ElasticIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public ElasticIO(Client client) {
        this.client = client;
    }

    public ElasticIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

//    private static DiscoveryResult convertHitToMetricDiscoveryResult(Map<String, Object> source) {
//        String metricName = (String)source.get(METRIC_NAME.toString());
//        String tenantId = (String)source.get(TENANT_ID.toString());
//        String unit = (String)source.get(UNIT.toString());
//        DiscoveryResult result = new DiscoveryResult(tenantId, metricName, unit);
//
//        return result;
//    }

    @Override
    public void insertDiscovery(List<Metric> batch) throws IOException {
        // TODO: check bulk insert result and retry
        BulkRequestBuilder bulk = client.prepareBulk();
        for (Metric metric : batch) {
            Locator locator = metric.getLocator();
            DiscoveryBuilder md = new DiscoveryBuilder(locator.getTenantId(), locator.getMetricName());
//            Map<String, Object> info = new HashMap<String, Object>();
            if (metric.getUnit() != null) { // metric units may be null
                md.setUnit(metric.getUnit());
//                info.put(UNIT.toString(), metric.getUnit());
            }
            md.setType(metric.getType().toString());
//            info.put(TYPE.toString(), metric.getType());
//            md.withAnnotation(info);
            bulk.add(createSingleRequest(md));
        }
        bulk.execute().actionGet();
    }

    private IndexRequestBuilder createSingleRequest(DiscoveryBuilder md) throws IOException {
        if (md.getMetricName() == null) {
            throw new IllegalArgumentException("trying to insert metric discovery without a metricName");
        }
        return client.prepareIndex(getIndex(md.getTenantId()), ES_TYPE)
                .setId(md.getDocumentId())
                .setCreate(true) // enables put-if-absent functionality
                .setSource(mapper.writeValueAsString(md));
//                .setSource(md.createSourceContent());
    }

    private String getIndex(String tenantId) {
        return INDEX_PREFIX + String.valueOf(Util.computeShard(tenantId));
    }

    private static QueryBuilder createQuery(DiscoveryBuilder md) {
        BoolQueryBuilder qb = boolQuery()
                .must(termQuery(TENANT_ID.toString(), md.getTenantId()));
        String metricName = md.getMetricName();
        if (!metricName.equals("*")) {
            if (metricName.contains("*")) {
                qb.must(wildcardQuery(METRIC_NAME + ".raw", metricName));
            } else {
                qb.must(termQuery(METRIC_NAME + ".raw", metricName));
            }
        }
        if (md.getUnit() != null && !md.getUnit().isEmpty()) {
            qb.must(termQuery(UNIT, md.getUnit()));
        }
        if (md.getType() != null && !md.getType().isEmpty()) {
            qb.must(termQuery(TYPE, md.getType()));
        }
        return qb;
    }

    @Override
    public List<DiscoveryResult> getMetricsWithNameLike(String tenantId, String query) {
        return search(new DiscoveryBuilder(tenantId, query));
    }

    public List<DiscoveryResult> getMetricsWithUnitAndType(String tenantId, String unit) {
        return search(new DiscoveryBuilder(tenantId).setUnit(unit));
    }

    public List<DiscoveryResult> getMetricsWithUnit(String tenantId, String unit) {
        return search(new DiscoveryBuilder(tenantId).setUnit(unit));
    }

    public List<DiscoveryResult> getMetricsWithType(String tenantId, String type) {
        return search(new DiscoveryBuilder(tenantId).setUnit(type));
    }

    @Override
    public List<DiscoveryResult> getMetrics(String tenantId) {
        return search(new DiscoveryBuilder(tenantId, "*"));
    }

    @Override
    public DiscoveryResult getMetric(String tenantId, String metricName) {
        DiscoveryBuilder md = new DiscoveryBuilder(tenantId, metricName);
        GetResponse metricDoc = client.prepareGet(getIndex(md.getTenantId()), ES_TYPE, md.getDocumentId())
                .setOperationThreaded(false)
                .execute()
                .actionGet();
        try {
            return mapper.readValue(metricDoc.getSourceAsBytes(), DiscoveryResult.class);
        } catch (IOException e) {
            log.error("Error deserializing result into DiscoveryResult bean", e);
//            e.printStackTrace();
            return null;
        }
//        return convertHitToMetricDiscoveryResult(metricDoc.getSource());
    }


    public List<DiscoveryResult> search(DiscoveryBuilder md) {
        List<DiscoveryResult> result = new ArrayList<DiscoveryResult>();
        QueryBuilder query = createQuery(md);
        Timer.Context searchTimerCtx = searchTimer.time();
        SearchResponse searchRes = client.prepareSearch(getIndex(md.getTenantId()))
                .setSize(500)
                .setVersion(true)
                .setQuery(query)
                .execute()
                .actionGet();
        searchTimerCtx.stop();
        for (SearchHit hit : searchRes.getHits().getHits()) {
            try {
                DiscoveryResult entry = mapper.readValue(hit.getSourceAsString(), DiscoveryResult.class);
                result.add(entry);
            } catch (IOException e) {
                log.error("Error deserializing result into DiscoveryResult bean", e);
            }
//            DiscoveryResult entry = convertHitToMetricDiscoveryResult(hit.getSource());
        }
        return result;
    }

    //    public class Discovery implements Serializable {
//        private String metricName;
//        private String tenantId;
//        private String unit;
//        private String type;
//
//        public Discovery(String tenantId) {
//            this.tenantId = tenantId;
//        }
//
//        public Discovery(String tenantId, String metricName) {
//            this.tenantId = tenantId;
//            this.metricName = metricName;
//        }
//
//        public Discovery setMetricName(String metricName) {
//            this.metricName = metricName;
//            return this;
//        }
//
//        public Discovery setTenantId(String tenantId) {
//            this.tenantId = tenantId;
//            return this;
//        }
//
//        public Discovery setUnit(String unit) {
//            this.unit = unit;
//            return this;
//        }
//
//        public Discovery setType(String type) {
//            this.type = type;
//            return this;
//        }
//
//        public String getUnit() {
//            return unit;
//        }
//
//        public String getType() {
//            return type;
//        }
//
//        public String getTenantId() {
//            return tenantId;
//        }
//
//        public String getMetricName() {
//            return metricName;
//        }
//
//        public String getDocumentId() {
//            return tenantId + ":" + metricName;
//        }
//    }

}
