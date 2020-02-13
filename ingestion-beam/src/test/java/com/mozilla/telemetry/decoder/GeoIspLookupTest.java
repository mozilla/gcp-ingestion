package com.mozilla.telemetry.decoder;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.mozilla.telemetry.options.InputFileFormat;
import com.mozilla.telemetry.options.OutputFileFormat;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GeoIspLookupTest {

  private static final String MMDB = "src/test/resources/ispDB/GeoIP2-ISP-Test.mmdb";

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testOutput() {
    // Some of the IPs below are chosen specifically because they are contained in the test city
    // database; see the json source for the test db in:
    // https://github.com/maxmind/MaxMind-DB/blob/664aeeb08bb50f53a1fdceac763c37f6465e44a4/source-data/GeoIP2-City-Test.json
    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}", //
        "{\"attributeMap\":{\"remote_addr\":\"24.38.243.141\"},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"remote_addr\":\"10.0.0.2\"" //
            + ",\"x_forwarded_for\":\"192.168.1.2, 23.32.32.1, 23.32.32.11\"" //
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(//
        "{\"attributeMap\":" //
            + "{\"host\":\"test\"" //
            + ",\"isp_db_version\":\"2018-01-15T22:27:16Z\"" //
            + "},\"payload\":\"dGVzdA==\"}", //
        "{\"attributeMap\":" //
            + "{\"isp_name\":\"Akamai Technologies\"" //
            + ",\"remote_addr\":\"10.0.0.2\"" //
            + ",\"isp_db_version\":\"2018-01-15T22:27:16Z\"" //
            + ",\"isp_organization\":\"Akamai Technologies\"" //
            + ",\"x_forwarded_for\":\"192.168.1.2, 23.32.32.1, 23.32.32.11\"}" //
            + ",\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"remote_addr\":\"24.38.243.141\"" //
            + ",\"isp_db_version\":\"2018-01-15T22:27:16Z\"" //
            + ",\"isp_organization\":\"LAWN MULLEN & GOOD INTERNATIONAL\"}" + ",\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(GeoIspLookup.of(pipeline.newProvider(MMDB))).apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    GeoIspLookup.clearSingletonsForTests();
    final PipelineResult result = pipeline.run();

    final List<MetricResult<Long>> counters = Lists.newArrayList(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(GeoIspLookup.Fn.class)).build())
        .getCounters());

    assertEquals(1, counters.size());
    counters.forEach(counter -> assertThat(counter.getCommitted(), greaterThan(0L)));
  }

  @Test
  public void testThrowsOnMissingIspDatabase() {
    thrown.expectCause(IsInstanceOf.instanceOf(UncheckedIOException.class));

    final List<String> input = Collections
        .singletonList("{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}");

    pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()) //
        .apply(GeoIspLookup.of(pipeline.newProvider("missing-file.mmdb")));

    GeoIspLookup.clearSingletonsForTests();
    pipeline.run();
  }
}
