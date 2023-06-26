package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import com.mozilla.telemetry.util.TestWithDeterministicJson;
import java.util.Arrays;
import java.util.List;
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
        .apply(ParseProxy.of()) //
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
        .apply(ParseProxy.of()) //
        .apply(GeoCityLookup.of("src/test/resources/cityDB/GeoIP2-City-Test.mmdb", null))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
