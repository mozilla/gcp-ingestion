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
  public void testIngestionClientIpCounters() {
    final List<String> input = Arrays.asList(//
        // via fastly: client, fastly-appended client, fastly edge, forwarding rule, lb
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"" //
            + ",\"x_forwarded_for\":\"6, 5, 151.101.1.1, 2, 1\"},\"payload\":\"\"}",
        // via fastly, client ip not appended to x-forwarded-for
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"" //
            + ",\"x_forwarded_for\":\"151.101.1.1, 2, 1\"},\"payload\":\"\"}",
        // direct
        "{\"attributeMap\":{\"x_forwarded_for\":\"4, 3, 2, 1\"},\"payload\":\"\"}",
        // header without x-forwarded-for is still removed
        "{\"attributeMap\":{\"x_ingestion_client_ip\":\"5\"},\"payload\":\"\"}");

    // x-forwarded-for is handled exactly as before; only the header is removed
    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"x_forwarded_for\":\"6,5,151.101.1.1\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_forwarded_for\":\"151.101.1.1\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_forwarded_for\":\"4,3\"},\"payload\":\"\"}",
        "{\"attributeMap\":{},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of(null)) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();

    final String prefix = "unknown_namespace/unknown_doctype/";
    Assert.assertEquals(2, counterValue(result, prefix + "fastly_path"));
    Assert.assertEquals(1, counterValue(result, prefix + "fastly_path_client_ip_in_xff"));
    Assert.assertEquals(1, counterValue(result, prefix + "fastly_path_client_ip_not_in_xff"));
    Assert.assertEquals(1, counterValue(result, prefix + "direct_path"));
  }

  @Test
  public void testIngestionLbTagsReplaceLbTags() {
    final List<String> input = Arrays.asList(//
        // via fastly: the load balancer only sees fastly's tls connection
        "{\"attributeMap\":{\"x_ingestion_lb_tags\":\"TLSv1, ECDHE-RSA-AES128-SHA, v2\"" //
            + ",\"x_lb_tags\":\"TLSv1.3, 1302, v2\"},\"payload\":\"\"}",
        // via fastly, plain http from the client
        "{\"attributeMap\":{\"x_ingestion_lb_tags\":\", , v2\"" //
            + ",\"x_lb_tags\":\"TLSv1.3, 1302, v2\"},\"payload\":\"\"}",
        // blank header is ignored and removed
        "{\"attributeMap\":{\"x_ingestion_lb_tags\":\" \"" //
            + ",\"x_lb_tags\":\"TLSv1.2, c02f, v2\"},\"payload\":\"\"}",
        // direct
        "{\"attributeMap\":{\"x_lb_tags\":\"TLSv1.2, c02f, v2\"},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":{\"x_lb_tags\":\"TLSv1, ECDHE-RSA-AES128-SHA, v2\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_lb_tags\":\", , v2\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_lb_tags\":\"TLSv1.2, c02f, v2\"},\"payload\":\"\"}",
        "{\"attributeMap\":{\"x_lb_tags\":\"TLSv1.2, c02f, v2\"},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(ParseProxy.of(null)) //
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  private static long counterValue(PipelineResult result, String name) {
    return StreamSupport.stream(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(KeyedCounter.class, name)).build())
        .getCounters().spliterator(), false).mapToLong(MetricResult::getCommitted).sum();
  }
}
