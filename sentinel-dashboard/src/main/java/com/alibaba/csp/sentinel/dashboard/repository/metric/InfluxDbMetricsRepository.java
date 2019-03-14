/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.dashboard.repository.metric;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.MetricEntity;
import com.alibaba.csp.sentinel.util.StringUtil;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Caches metrics data in a period of time in memory.
 *
 * @author Carpenter Lee
 * @author Eric Zhao
 */
@Component
public class InfluxDbMetricsRepository implements MetricsRepository<MetricEntity> {

    private static final long MAX_METRIC_LIVE_TIME_MS = 1000 * 60 * 5;

    /**
     * {@code app -> resource -> timestamp -> metric}
     */
    @Autowired
    InfluxDB influxDB;


    @Override
    public synchronized void save(MetricEntity entity) {
        if (entity == null || StringUtil.isBlank(entity.getApp())) {
            return;
        }
        entity.setTimestampLong(entity.getTimestamp().getTime());
        entity.setGmtModifiedLong(entity.getGmtModified().getTime());
        entity.setGmtCreateLong(entity.getGmtCreate().getTime());
        Point point = Point.measurementByPOJO(MetricEntity.class).addFieldsFromPOJO(entity).build();
        influxDB.write(point);
    }

    @Override
    public synchronized void saveAll(Iterable<MetricEntity> metrics) {
        if (metrics == null) {
            return;
        }
        BatchPoints build = BatchPoints.database("sentinel").build();
        metrics.forEach(mt -> {
            mt.setTimestampLong(mt.getTimestamp().getTime());
            mt.setGmtModifiedLong(mt.getGmtModified().getTime());
            mt.setGmtCreateLong(mt.getGmtCreate().getTime());
            Point point = Point.measurementByPOJO(MetricEntity.class).addFieldsFromPOJO(mt).build();
            build.point(point);
        });
        influxDB.write(build);
    }

    @Override
    public synchronized List<MetricEntity> queryByAppAndResourceBetween(String app, String resource,
                                                                        long startTime, long endTime) {
        List<MetricEntity> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }
        Query query = new Query("select * from sentinel_metric where app='" + app + "' and resource='" + resource + "' and timestampLong>=" + startTime + " and timestampLong<=" + endTime, "sentinel");

        QueryResult queryResult = influxDB.query(query);
        InfluxDBResultMapper influxDBResultMapper = new InfluxDBResultMapper();
        results = influxDBResultMapper.toPOJO(queryResult,MetricEntity.class);
        results.forEach(x->{
            x.setGmtCreate(new Date(x.getGmtCreateLong()));
            x.setGmtModified(new Date(x.getGmtModifiedLong()));
            x.setTimestamp(new Date(x.getTimestampLong()));
        });
        return results;
    }

    /**
     * 根据属性名set属性值
     */
    private void setFieldValueByName(String fieldName, Object o, Object value) {
        try {
            Field f = o.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(o, value);
        } catch (Exception e) {

        }
    }


    @Override
    public List<String> listResourcesOfApp(String app) {
        List<String> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }
        final long minTimeMs = System.currentTimeMillis() - 1000 * 60;
        // resource -> timestamp -> metric
        Query query = new Query("select * from sentinel_metric where app='" + app + "' and timestampLong>=" + minTimeMs, "sentinel");
        List<MetricEntity> results2 = new ArrayList<>();
        QueryResult queryResult = influxDB.query(query);
        InfluxDBResultMapper influxDBResultMapper = new InfluxDBResultMapper();
        results2 = influxDBResultMapper.toPOJO(queryResult,MetricEntity.class);
        results2.forEach(x->{
            x.setGmtCreate(new Date(x.getGmtCreateLong()));
            x.setGmtModified(new Date(x.getGmtModifiedLong()));
            x.setTimestamp(new Date(x.getTimestampLong()));
        });
        Map<String, MetricEntity> resourceCount = new ConcurrentHashMap<>(32);

        for (MetricEntity newEntity : results2) {
            if (resourceCount.containsKey(newEntity.getResource())) {
                MetricEntity oldEntity = resourceCount.get(newEntity.getResource());
                oldEntity.addPassQps(newEntity.getPassQps());
                oldEntity.addRtAndSuccessQps(newEntity.getRt(), newEntity.getSuccessQps());
                oldEntity.addBlockQps(newEntity.getBlockQps());
                oldEntity.addExceptionQps(newEntity.getExceptionQps());
                oldEntity.addCount(1);
            } else {
                resourceCount.put(newEntity.getResource(), MetricEntity.copyOf(newEntity));
            }
        }
        // Order by last minute b_qps DESC.
        return resourceCount.entrySet()
                .stream()
                .sorted((o1, o2) -> {
                    MetricEntity e1 = o1.getValue();
                    MetricEntity e2 = o2.getValue();
                    int t = e2.getBlockQps().compareTo(e1.getBlockQps());
                    if (t != 0) {
                        return t;
                    }
                    return e2.getPassQps().compareTo(e1.getPassQps());
                })
                .map(Entry::getKey)
                .collect(Collectors.toList());
    }
}
