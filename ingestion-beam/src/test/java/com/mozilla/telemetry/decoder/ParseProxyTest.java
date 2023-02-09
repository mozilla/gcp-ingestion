package com.mozilla.telemetry.decoder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

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
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ParseProxyTest extends TestWithDeterministicJson {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testOutput() {
    final List<String> input = Arrays.asList(//
        // Note that payloads are interpreted as base64 strings, so we sometimes add '+'
        // to pad them out to be valid base64.
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
            + ",\"x_forwarded_for\":\"4, 3, 6.7.8.9, 1\"" //
            + "},\"payload\":\"staticProxied+++\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 8.8.4.1, 1\"" //
            + "},\"payload\":\"googleIPv4Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2c0f:fb50::1, 1\"" //
            + "},\"payload\":\"googleIPv6Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 3.2.34.1, 1\"" //
            + "},\"payload\":\"cloudFrontIPv4Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2600:1ff2:4000::1, 1\"" //
            + "},\"payload\":\"cloudFrontIPv6Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2, 1\"" //
            + ",\"x_pipeline_proxy\":\"1999-12-31T23:59:59.999999Z\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}",
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"proxy_list_versions\":\"test\"" //
            + ",\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2\"" //
            + "},\"payload\":\"retried+\"}");

    final String version = "goog.json:2023-02-13T14:04:31.200094" //
        + ";cloud.json:2023-02-13T14:04:31.200094" //
        + ";cloudfront:1676326387:2023-02-13-22-13-07";
    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + "},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + "},\"payload\":\"proxied+\"}", //
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + "},\"payload\":\"emptyXPP\"}", //
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"3, 2, 1\"" //
            + "},\"payload\":\"notProxied++\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"staticProxied+++\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_detected\":\"true\"" //
            + ",\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"googleIPv4Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_detected\":\"true\"" //
            + ",\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"googleIPv6Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_detected\":\"true\"" //
            + ",\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"cloudFrontIPv4Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_detected\":\"true\"" //
            + ",\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"cloudFrontIPv6Subnet\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"" + version + "\"" //
            + ",\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"x_forwarded_for\":\"4,3,1\"" //
            + "},\"payload\":\"proxiedWithTimestamp\"}",
        "{\"attributeMap\":" //
            + "{\"proxy_list_versions\":\"test\"" //
            + ",\"proxy_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"submission_timestamp\":\"1999-12-31T23:59:59.999999Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2\"" //
            + "},\"payload\":\"retried+\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of("2.3.4.5,6.7.8.9", "src/test/resources/google-ip-ranges",
            "src/test/resources/cloudfront-ip-ranges.json")) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    ParseProxy.clearSingletonsForTests();
    final PipelineResult result;
    try {
      result = pipeline.run();
    } finally {
      // clear singletons to prevent impacting other tests
      ParseProxy.clearSingletonsForTests();
    }

    final List<MetricResult<Long>> counters = Lists.newArrayList(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(ParseProxy.Fn.class)).build())
        .getCounters());

    assertEquals(4, counters.size());
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
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of(null, null, null)) //
        .apply(GeoCityLookup.of("src/test/resources/cityDB/GeoIP2-City-Test.mmdb", null))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    ParseProxy.clearSingletonsForTests();
    try {
      pipeline.run();
    } finally {
      // clear singletons to prevent impacting other tests
      ParseProxy.clearSingletonsForTests();
    }
  }
}
