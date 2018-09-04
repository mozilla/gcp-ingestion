/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.SerializationConfig;
import org.junit.Rule;
import org.junit.Test;

public class SinkTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void decodeJsonAndEncodeJson() {
    // Without setting this feature, the ordering of properties in output JSON is non-deterministic.
    Sink.objectMapper.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);

    final List<String> input = Arrays.asList(
            "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
            "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
            "{\"payload\":\"dGVzdA==\"}",
            "{\"payload\":\"\"}");

    final List<String> expected = Arrays.asList(
            "{\"attributeMap\":{\"host\":\"test\"},\"payload\":\"dGVzdA==\"}",
            "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
            "{\"attributeMap\":null,\"payload\":\"dGVzdA==\"}",
            "{\"attributeMap\":null,\"payload\":\"\"}");

    PCollection<String> output = pipeline
        .apply(Create.of(input))
        .apply(Sink.decodeJson)
        .get(Sink.decodeJson.mainTag)
        .apply(Sink.encodeJson);

    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }
}
