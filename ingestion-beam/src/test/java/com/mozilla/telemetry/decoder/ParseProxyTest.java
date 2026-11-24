package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.metrics.KeyedCounter;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
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
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2, 1\"" //
            + "},\"payload\":\"test\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"submission_timestamp\":\"2000-01-01T00:00:00.000000Z\"" //
            + ",\"x_forwarded_for\":\"4,3\"" //
            + "},\"payload\":\"test\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of(null)) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testWithGeoCityLookup() {
    final List<String> input = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"x_forwarded_for\":\"_, 202.196.224.0, _, _\"" //
            + "},\"payload\":\"test\"}",
        "{\"attributeMap\":" //
            + "{\"x_pipeline_proxy\":1" //
            + ",\"x_forwarded_for\":\"_, 202.196.224.0, _, _\"" //
            + "},\"payload\":\"ignorePipelineProxy+\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" //
            + "},\"payload\":\"test\"}",
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" //
            + ",\"x_pipeline_proxy\":\"1\"" //
            + "},\"payload\":\"ignorePipelineProxy+\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of(null)) //
        .apply(GeoCityLookup.of("src/test/resources/cityDB/GeoIP2-City-Test.mmdb", null))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testGeoipSkip() {
    final List<String> input = Arrays.asList(//
        // Note that payloads are interpreted as base64 strings, so we sometimes add '+'
        // to pad them out to be valid base64.
        "{\"attributeMap\":{\"x_forwarded_for\":\"4, 3, 2, 1\"},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"document_namespace\":\"test\"" //
            + ",\"document_type\":\"geoip-skip\"" //
            + ",\"document_version\":\"1\"" //
            + ",\"x_forwarded_for\":\"4, 3, 2, 1\"" //
            + "},\"payload\":\"test\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"x_forwarded_for\":\"4,3\"},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"document_namespace\":\"test\"" //
            + ",\"document_type\":\"geoip-skip\"" //
            + ",\"document_version\":\"1\"" //
            + ",\"x_forwarded_for\":\"4\"" //
            + "},\"payload\":\"test\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of("schemas.tar.gz")) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testOhttpMismatchCounters() {
    final String ohttpAttributes = "\"document_namespace\":\"test\"" //
        + ",\"document_type\":\"ohttp\"" //
        + ",\"document_version\":\"1\"" //
        + ",\"x_forwarded_for\":\"4, 3, 2, 1\"";
    final List<String> input = Arrays.asList(//
        // submitted through the OHTTP gateway, so no user agent
        "{\"attributeMap\":{" + ohttpAttributes + "},\"payload\":\"\"}", //
        // submitted directly
        "{\"attributeMap\":{" + ohttpAttributes + ",\"user_agent\":\"Firefox\"}" //
            + ",\"payload\":\"\"}", //
        // doesn't declare ohttp
        "{\"attributeMap\":" //
            + "{\"document_namespace\":\"test\"" //
            + ",\"document_type\":\"geoip-skip\"" //
            + ",\"document_version\":\"1\"" //
            + ",\"user_agent\":\"Firefox\"" //
            + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of("schemas.tar.gz")) //
        .apply(OutputFileFormat.json.encode());

    // the IP is kept, so geo and ISP lookups still run
    PAssert.that(output).satisfies(messages -> {
      messages.forEach(m -> {
        if (m.contains("\"ohttp\"")) {
          Assert.assertTrue(m, m.contains("\"x_forwarded_for\":\"4,3\""));
        }
      });
      return null;
    });

    final PipelineResult result = pipeline.run();

    Assert.assertEquals(2, counterValue(result, "test/ohttp_v1/ohttp_declared"));
    Assert.assertEquals(1, counterValue(result, "test/ohttp_v1/ohttp_declared_direct_submission"));
    Assert.assertEquals(0, counterValue(result, "test/geoip_skip_v1/ohttp_declared"));
  }

  @Test
  public void testIngestionClientIp() {
    final String attributes = "\"document_namespace\":\"test\"" //
        + ",\"document_type\":\"geoip-skip\"" //
        + ",\"document_version\":\"1\"";
    final List<String> input = Arrays.asList(//
        // via fastly: client, fastly-appended client, fastly edge, forwarding rule, lb
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"" //
            + ",\"x_forwarded_for\":\"6, 5, 151.101.1.1, 2, 1\"},\"payload\":\"\"}",
        // via fastly, client ip not appended to x-forwarded-for
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"" //
            + ",\"x_forwarded_for\":\"151.101.1.1, 2, 1\"},\"payload\":\"\"}",
        // via fastly, with geoip_skip_entries=1 applied after the fastly hop is handled
        "{\"attributeMap\":{" + attributes + ",\"x_ingestion_client_ip\":\"5\"" //
            + ",\"x_forwarded_for\":\"6, 5, 151.101.1.1, 2, 1\"},\"payload\":\"\"}",
        // direct: unchanged behavior
        "{\"attributeMap\":{\"x_forwarded_for\":\"4, 3, 2, 1\"},\"payload\":\"\"}",
        // header without x-forwarded-for is still removed
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"x_forwarded_for\":\"6,5\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_forwarded_for\":\"5\"},\"payload\":\"\"}",
        "{\"attributeMap\":{" + attributes + ",\"x_forwarded_for\":\"6\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_forwarded_for\":\"4,3\"},\"payload\":\"\"}",
        "{\"attributeMap\":{},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of("schemas.tar.gz")) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();

    Assert.assertEquals(2, counterValue(result, "unknown_namespace/unknown_doctype/fastly_path"));
    Assert.assertEquals(1, counterValue(result, "test/geoip_skip_v1/fastly_path"));
    Assert.assertEquals(1, counterValue(result, "unknown_namespace/unknown_doctype/direct_path"));
  }

  private static long counterValue(PipelineResult result, String name) {
    return StreamSupport.stream(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(KeyedCounter.class, name)).build())
        .getCounters().spliterator(), false).mapToLong(MetricResult::getCommitted).sum();
  }
}
