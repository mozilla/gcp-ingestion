/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class ParseProxyTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"x_pipeline_proxy\":1" //
            + "},\"payload\":\"proxied+\"}",
        "{\"attributeMap\":" //
            + "{\"x_pipeline_proxy\":\"\"" //
            + "},\"payload\":\"emptyXPP\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"3, 2, 1\"" //
            + "},\"payload\":\"notProxied++\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2, 1\"" //
            + ",\"x_pipeline_proxy\":\"1999-12-31T23:59:59.999999Z\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2\"" //
            + "},\"payload\":\"retried+\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":{},\"payload\":\"proxied+\"}", //
        "{\"attributeMap\":{},\"payload\":\"emptyXPP\"}", //
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"3, 2, 1\"" //
            + "},\"payload\":\"notProxied++\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2\"" //
            + "},\"payload\":\"retried+\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(ParseProxy.of()) //
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(m -> sortJson(OutputFileFormat.json.encodeSingleMessage(m))));

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();

    final List<MetricResult<Long>> counters = Lists.newArrayList(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(ParseProxy.Fn.class)).build())
        .getCounters());

    assertEquals(1, counters.size());
    counters.forEach(counter -> assertThat(counter.getCommitted(), greaterThan(0L)));
  }

  @Test
  public void testWithGeoCityLookup() {
    final List<String> input = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"x_forwarded_for\":\"_, 202.196.224.0, _\"" //
            + "},\"payload\":\"notProxied++\"}",
        "{\"attributeMap\":" //
            + "{\"x_pipeline_proxy\":1" //
            + ",\"x_forwarded_for\":\"_, 202.196.224.0, _, _\"" //
            + "},\"payload\":\"proxied+\"}",
        "{\"attributeMap\":" //
            + "{\"x_pipeline_proxy\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"_, 202.196.224.0, _, _\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"geo_db_version\":\"2019-01-03T21:26:19Z\"},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" //
            + "},\"payload\":\"notProxied++\"}",
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" //
            + "},\"payload\":\"proxied+\"}",
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(ParseProxy.of()) //
        .apply(GeoCityLookup
            .of(pipeline.newProvider("src/test/resources/cityDB/GeoIP2-City-Test.mmdb"), null))
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(m -> sortJson(OutputFileFormat.json.encodeSingleMessage(m))));

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();
  }
}
