/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.mozilla.telemetry.transforms.GeoCityLookup;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.SerializationConfig;
import org.junit.Rule;
import org.junit.Test;

public class ValidateTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void validate() {
    // Without setting this feature, the ordering of properties in output JSON is non-deterministic.
    Sink.objectMapper.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);

    final List<String> input = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"remote_addr\":\"8.8.8.8\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":"
            + "{\"remote_addr\":\"10.0.0.2\""
            + ",\"x_forwarded_for\":\"192.168.1.2, 63.245.208.195\""
            + "},\"payload\":\"dGVzdA==\"}");

    final List<String> expected = Arrays.asList(
        "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":{\"geo_country\":\"US\"},\"payload\":\"dGVzdA==\"}",
        "{\"attributeMap\":"
            + "{\"geo_country\":\"US\""
            + ",\"geo_city\":\"Sacramento\""
            + ",\"geo_subdivision1\":\"CA\""
            + "},\"payload\":\"dGVzdA==\"}");

    final PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply("decodeJson", Validate.decodeJson)
        .get(Validate.decodeJson.mainTag)
        .apply("geoCityLookup", new GeoCityLookup("GeoLite2-City.mmdb"))
        .apply("encodeJson", Validate.encodeJson);

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
