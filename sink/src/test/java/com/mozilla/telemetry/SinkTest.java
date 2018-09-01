/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class SinkTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void decodeJsonAndEncodeJson() {
    PCollection<String> output = pipeline
        .apply(Create.of(Arrays.asList(
            "{\"payload\":\"dGVzdA==\",\"attributeMap\":{\"host\":\"test\"}}",
            "{\"payload\":\"dGVzdA==\",\"attributeMap\":null}",
            "{\"payload\":\"dGVzdA==\"}",
            "{\"payload\":\"\"}"
        )))
        .apply(Sink.decodeJson)
        .get(Sink.decodeJson.mainTag)
        .apply(Sink.encodeJson);

    PAssert.that(output).containsInAnyOrder(Arrays.asList(
        "{\"payload\":\"dGVzdA==\",\"attributeMap\":{\"host\":\"test\"}}",
        "{\"payload\":\"dGVzdA==\",\"attributeMap\":null}",
        "{\"payload\":\"dGVzdA==\",\"attributeMap\":null}",
        "{\"payload\":\"\",\"attributeMap\":null}"
    ));

    pipeline.run();
  }
}
