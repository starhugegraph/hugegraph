/*
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

package com.baidu.hugegraph.metric;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.Version;
import org.apache.tinkerpop.shaded.jackson.databind.Module;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleSerializers;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * Copy from com.codahale.metrics.json.MetricsModule
 */
public class MetricsModule extends Module {

    private static final Version VERSION = new Version(1, 0, 0, "",
                                                       "com.baidu.hugegraph",
                                                       "hugegraph-api");

    private static class GaugeSerializer extends StdSerializer<Gauge> {

        private GaugeSerializer() {
            super(Gauge.class);
        }

        @Override
        public void serialize(Gauge gauge, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Object value;
            try {
                value = gauge.getValue();
                json.writeObjectField("value", value);
            } catch (RuntimeException e) {
                json.writeObjectField("error", e.toString());
            }
            json.writeEndObject();
        }
    }

    private static class CounterSerializer extends StdSerializer<Counter> {

        private CounterSerializer() {
            super(Counter.class);
        }

        @Override
        public void serialize(Counter counter, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeNumberField("count", counter.getCount());
            json.writeEndObject();
        }
    }

    private static class HistogramSerializer extends StdSerializer<Histogram> {

        private final boolean showSamples;

        private HistogramSerializer(boolean showSamples) {
            super(Histogram.class);
            this.showSamples = showSamples;
        }

        @Override
        public void serialize(Histogram histogram, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Snapshot snapshot = histogram.getSnapshot();
            json.writeNumberField("count", histogram.getCount());
            json.writeNumberField("max", snapshot.getMax());
            json.writeNumberField("mean", snapshot.getMean());
            json.writeNumberField("min", snapshot.getMin());
            json.writeNumberField("p50", snapshot.getMedian());
            json.writeNumberField("p75", snapshot.get75thPercentile());
            json.writeNumberField("p95", snapshot.get95thPercentile());
            json.writeNumberField("p98", snapshot.get98thPercentile());
            json.writeNumberField("p99", snapshot.get99thPercentile());
            json.writeNumberField("p999", snapshot.get999thPercentile());

            if (showSamples) {
                json.writeObjectField("values", snapshot.getValues());
            }

            json.writeNumberField("stddev", snapshot.getStdDev());
            json.writeEndObject();
        }
    }

    private static class MeterSerializer extends StdSerializer<Meter> {

        private final String rateUnit;
        private final double rateFactor;

        public MeterSerializer(TimeUnit rateUnit) {
            super(Meter.class);
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = calculateRateUnit(rateUnit, "events");
        }

        @Override
        public void serialize(Meter meter, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeNumberField("count", meter.getCount());
            json.writeNumberField("m15_rate", meter.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("m1_rate", meter.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", meter.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", meter.getMeanRate() * rateFactor);
            json.writeStringField("units", rateUnit);
            json.writeEndObject();
        }
    }

    private static class TimerSerializer extends StdSerializer<Timer> {

        private final String rateUnit;
        private final double rateFactor;
        private final String durationUnit;
        private final double durationFactor;
        private final boolean showSamples;

        private TimerSerializer(TimeUnit rateUnit, TimeUnit durationUnit,
                                boolean showSamples) {
            super(Timer.class);
            this.rateUnit = calculateRateUnit(rateUnit, "calls");
            this.rateFactor = rateUnit.toSeconds(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.showSamples = showSamples;
        }

        @Override
        public void serialize(Timer timer, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            final Snapshot snapshot = timer.getSnapshot();
            json.writeNumberField("count", timer.getCount());
            json.writeNumberField("max", snapshot.getMax() * durationFactor);
            json.writeNumberField("mean", snapshot.getMean() * durationFactor);
            json.writeNumberField("min", snapshot.getMin() * durationFactor);

            json.writeNumberField("p50", snapshot.getMedian() * durationFactor);
            json.writeNumberField("p75", snapshot.get75thPercentile() * durationFactor);
            json.writeNumberField("p95", snapshot.get95thPercentile() * durationFactor);
            json.writeNumberField("p98", snapshot.get98thPercentile() * durationFactor);
            json.writeNumberField("p99", snapshot.get99thPercentile() * durationFactor);
            json.writeNumberField("p999", snapshot.get999thPercentile() * durationFactor);

            if (showSamples) {
                final long[] values = snapshot.getValues();
                final double[] scaledValues = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    scaledValues[i] = values[i] * durationFactor;
                }
                json.writeObjectField("values", scaledValues);
            }

            json.writeNumberField("stddev", snapshot.getStdDev() * durationFactor);
            json.writeNumberField("m15_rate", timer.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("m1_rate", timer.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", timer.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", timer.getMeanRate() * rateFactor);
            json.writeStringField("duration_units", durationUnit);
            json.writeStringField("rate_units", rateUnit);
            json.writeEndObject();
        }
    }

    private static class MetricRegistrySerializer
                   extends StdSerializer<MetricRegistry> {

        private final MetricFilter filter;

        private MetricRegistrySerializer(MetricFilter filter) {
            super(MetricRegistry.class);
            this.filter = filter;
        }

        @Override
        public void serialize(MetricRegistry registry, JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("version", VERSION.toString());
            json.writeObjectField("gauges", registry.getGauges(filter));
            json.writeObjectField("counters", registry.getCounters(filter));
            json.writeObjectField("histograms", registry.getHistograms(filter));
            json.writeObjectField("meters", registry.getMeters(filter));
            json.writeObjectField("timers", registry.getTimers(filter));
            json.writeEndObject();
        }
    }

    private final TimeUnit rateUnit;
    private final TimeUnit durationUnit;
    private final boolean showSamples;
    private final MetricFilter filter;

    public MetricsModule(TimeUnit rateUnit, TimeUnit durationUnit,
                         boolean showSamples) {
        this(rateUnit, durationUnit, showSamples, MetricFilter.ALL);
    }

    public MetricsModule(TimeUnit rateUnit, TimeUnit durationUnit,
                         boolean showSamples, MetricFilter filter) {
        this.rateUnit = rateUnit;
        this.durationUnit = durationUnit;
        this.showSamples = showSamples;
        this.filter = filter;
    }

    @Override
    public String getModuleName() {
        return "metrics";
    }

    @Override
    public Version version() {
        return VERSION;
    }

    @Override
    public void setupModule(Module.SetupContext context) {
        context.addSerializers(new SimpleSerializers(Arrays.asList(
                new GaugeSerializer(),
                new CounterSerializer(),
                new HistogramSerializer(showSamples),
                new MeterSerializer(rateUnit),
                new TimerSerializer(rateUnit, durationUnit, showSamples),
                new MetricRegistrySerializer(filter)
        )));
    }

    private static String calculateRateUnit(TimeUnit unit, String name) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return name + '/' + s.substring(0, s.length() - 1);
    }
}
