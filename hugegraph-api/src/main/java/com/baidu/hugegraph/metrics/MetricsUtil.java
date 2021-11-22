/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.metrics;

import com.codahale.metrics.Snapshot;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class MetricsUtil {

    private static final MetricRegistry registry =
                                        MetricManager.INSTANCE.getRegistry();

    private static final String STRHELP = "# HELP ";
    private static final String STRTYPE = "# TYPE ";
    private static final String HISTOGRAMTYPE = "histogram";
    private static final String UNTYPED = "untyped";
    private static final String GAUGETYPE = "gauge";
    private static final String ENDLSTR = "\n";
    private static final String SPACESTR = " ";
    private static final String COUNTATTR = "{name=\"count\",} ";
    private static final String MINATTR = "{name=\"min\",} ";
    private static final String MAXATTR = "{name=\"max\",} ";
    private static final String MEANATTR = "{name=\"mean\",} ";
    private static final String STDDEVATTR = "{name=\"stddev\",} ";
    private static final String P50ATTR = "{name=\"p50\",} ";
    private static final String P75ATTR = "{name=\"p75\",} ";
    private static final String P95ATTR = "{name=\"p95\",} ";
    private static final String P98ATTR = "{name=\"p98\",} ";
    private static final String P99ATTR = "{name=\"p99\",} ";
    private static final String P999ATTR = "{name=\"p999\",} ";
    private static final String MEANRATEATRR = "{name=\"mean_rate\",} ";
    private static final String ONEMINRATEATRR = "{name=\"m1_rate\",} ";
    private static final String FIREMINRATEATRR = "{name=\"m5_rate\",} ";
    private static final String FIFTMINRATEATRR = "{name=\"m15_rate\",} ";

    public static <T> Gauge<T> registerGauge(Class<?> clazz, String name,
                                             Gauge<T> gauge) {
        return registry.register(MetricRegistry.name(clazz, name), gauge);
    }

    public static Counter registerCounter(Class<?> clazz, String name) {
        return registry.counter(MetricRegistry.name(clazz, name));
    }

    public static Histogram registerHistogram(Class<?> clazz, String name) {
        return registry.histogram(MetricRegistry.name(clazz, name));
    }

    public static Meter registerMeter(Class<?> clazz, String name) {
        return registry.meter(MetricRegistry.name(clazz, name));
    }

    public static Timer registerTimer(Class<?> clazz, String name) {
        return registry.timer(MetricRegistry.name(clazz, name));
    }

    public static void writePrometheus(StringBuilder promeMetrics,
                                       MetricRegistry registry) {
        // gauges
        registry.getGauges().forEach((key, gauge) -> {
            if (gauge != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STRHELP)
                        .append(helpName).append(ENDLSTR);
                promeMetrics.append(STRTYPE)
                        .append(helpName).append(SPACESTR + GAUGETYPE + ENDLSTR);
                promeMetrics.append(helpName).append(SPACESTR).append(gauge.getValue()).append(ENDLSTR);
            }
        });

        // histograms
        registry.getHistograms().forEach((key, histogram) -> {
            if (histogram != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STRHELP)
                        .append(helpName).append(ENDLSTR);
                promeMetrics.append(STRTYPE)
                        .append(helpName)
                        .append(SPACESTR + HISTOGRAMTYPE + ENDLSTR);

                promeMetrics.append(helpName)
                        .append(COUNTATTR).append(histogram.getCount()).append(ENDLSTR);
                promeMetrics.append(
                        exportSnapshort(helpName, histogram.getSnapshot()));
            }
        });

        // meters
        registry.getMeters().forEach((key, metric) -> {
            if (metric != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STRHELP)
                        .append(helpName).append(ENDLSTR);
                promeMetrics.append(STRTYPE)
                        .append(helpName)
                        .append(SPACESTR + HISTOGRAMTYPE + ENDLSTR);

                promeMetrics.append(helpName)
                        .append(COUNTATTR).append(metric.getCount()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(MEANRATEATRR).append(metric.getMeanRate()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(ONEMINRATEATRR).append(metric.getOneMinuteRate()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(FIREMINRATEATRR).append(metric.getFiveMinuteRate()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(FIFTMINRATEATRR).append(metric.getFifteenMinuteRate()).append(ENDLSTR);
            }
        });

        // timer
        registry.getTimers().forEach((key, timer) -> {
            if (timer != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STRHELP)
                        .append(helpName).append(ENDLSTR);
                promeMetrics.append(STRTYPE)
                        .append(helpName)
                        .append(SPACESTR + HISTOGRAMTYPE + ENDLSTR);

                promeMetrics.append(helpName)
                        .append(COUNTATTR).append(timer.getCount()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(ONEMINRATEATRR).append(timer.getOneMinuteRate()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(FIREMINRATEATRR).append(timer.getFiveMinuteRate()).append(ENDLSTR);
                promeMetrics.append(helpName)
                        .append(FIFTMINRATEATRR).append(timer.getFifteenMinuteRate()).append(ENDLSTR);
                promeMetrics.append(
                        exportSnapshort(helpName, timer.getSnapshot()));
            }
        });
    }

    private static String replaceDotDashInKey(String orgKey){
        return orgKey.replace(".", "_").replace("-", "_");
    }

    private static String exportSnapshort(final String helpName, final Snapshot snapshot){
        if ( snapshot != null ) {
            StringBuilder snapMetrics = new StringBuilder();
            snapMetrics.append(helpName)
                    .append(MINATTR).append(snapshot.getMin()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(MAXATTR).append(snapshot.getMax()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(MEANATTR).append(snapshot.getMean()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(STDDEVATTR).append(snapshot.getStdDev()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P50ATTR).append(snapshot.getMedian()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P75ATTR).append(snapshot.get75thPercentile()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P95ATTR).append(snapshot.get95thPercentile()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P98ATTR).append(snapshot.get98thPercentile()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P99ATTR).append(snapshot.get99thPercentile()).append(ENDLSTR);
            snapMetrics.append(helpName)
                    .append(P999ATTR).append(snapshot.get999thPercentile()).append(ENDLSTR);
            return  snapMetrics.toString();
        }
        return "";
    }

}
