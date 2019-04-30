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
import java.io.UncheckedIOException;
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
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GeoCityLookupTest {

  private static final String MMDB = "src/test/resources/cityDB/GeoIP2-City-Test.mmdb";

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
        "{\"attributeMap\":{\"remote_addr\":\"202.196.224.0\"},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"remote_addr\":\"10.0.0.2\"" //
            + ",\"x_forwarded_for\":\"192.168.1.2, 216.160.83.56, 60.1.1.1\"" //
            + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"" + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\""
            + "},\"payload\":\"dGVzdA==\"}", //
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"PH\"" //
            + ",\"normalized_country_code\":\"PH\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" + "},\"payload\":\"\"}", //
        "{\"attributeMap\":" //
            + "{\"geo_country\":\"US\"" //
            + ",\"normalized_country_code\":\"US\"" //
            + ",\"geo_city\":\"Milton\"" //
            + ",\"geo_subdivision1\":\"WA\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"" + "},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(GeoCityLookup.of(pipeline.newProvider(MMDB), null))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();

    final List<MetricResult<Long>> counters = Lists.newArrayList(result.metrics()
        .queryMetrics(MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(GeoCityLookup.Fn.class)).build())
        .getCounters());

    assertEquals(6, counters.size());
    counters.forEach(counter -> assertThat(counter.getCommitted(), greaterThan(0L)));
  }

  @Test
  public void testCityRejected() {
    final List<String> input = Arrays.asList("{\"attributeMap\":" //
        + "{\"remote_addr\":\"10.0.0.2\"" //
        + ",\"x_forwarded_for\":\"192.168.1.2, 216.160.83.56, 60.1.1.1\"" //
        + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":" + "{\"geo_country\":\"US\"" + ",\"normalized_country_code\":\"US\"" //
            + ",\"geo_subdivision1\":\"WA\"" //
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"},\"payload\":\"\"}");

    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(GeoCityLookup.of(pipeline.newProvider(MMDB),
            pipeline.newProvider("src/test/resources/cityFilters/sacramento.txt")))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();
  }

  @Test
  public void testCityAllowed() {
    final List<String> input = Arrays.asList("{\"attributeMap\":" //
        + "{\"remote_addr\":\"10.0.0.2\"" //
        + ",\"x_forwarded_for\":\"192.168.1.2, 216.160.83.56, 60.1.1.1\"" //
        + "},\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":" + "{\"geo_country\":\"US\"" + ",\"normalized_country_code\":\"US\"" //
            + ",\"geo_city\":\"Milton\",\"geo_subdivision1\":\"WA\""
            + ",\"geo_db_version\":\"2019-01-03T21:26:19Z\"},\"payload\":\"\"}");
    final PCollection<String> output = pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(GeoCityLookup.of(pipeline.newProvider(MMDB),
            pipeline.newProvider("src/test/resources/cityFilters/milton.txt")))
        .apply(OutputFileFormat.json.encode());

    PAssert.that(output).containsInAnyOrder(expected);

    final PipelineResult result = pipeline.run();
  }

  @Test
  public void testThrowsOnMissingCityDatabase() throws Exception {
    thrown.expectCause(IsInstanceOf.instanceOf(UncheckedIOException.class));

    final List<String> input = Arrays
        .asList("{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}");

    pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(GeoCityLookup.of(pipeline.newProvider("missing-file.mmdb"), null));

    pipeline.run();
  }

  @Test
  public void testThrowsOnMissingCityFilter() throws Exception {
    thrown.expectCause(IsInstanceOf.instanceOf(UncheckedIOException.class));

    final List<String> input = Arrays
        .asList("{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}");

    pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(
            GeoCityLookup.of(pipeline.newProvider(MMDB), pipeline.newProvider("missing-file.txt")));

    pipeline.run();
  }

  @Test
  public void testThrowsOnInvalidCityFilter() throws Exception {
    thrown.expectCause(IsInstanceOf.instanceOf(IllegalStateException.class));

    final List<String> input = Arrays
        .asList("{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}");

    pipeline //
        .apply(Create.of(input)) //
        .apply(InputFileFormat.json.decode()).output() //
        .apply(GeoCityLookup.of(pipeline.newProvider(MMDB),
            pipeline.newProvider("src/test/resources/cityFilters/invalid.txt")));

    pipeline.run();
  }

}
