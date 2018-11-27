/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;

public class ErrorOutputTypeTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void stdout() {
    SinkOptions.Parsed options = pipeline.getOptions().as(SinkOptions.Parsed.class);
    PubsubMessage message = new PubsubMessage(new byte[] {},
        ImmutableMap.of("purpose", "testing error output to stdout"));
    pipeline.apply(Create.of(message)).apply(ErrorOutputType.stdout.write(options));
    pipeline.run();
  }

  @Test
  public void stderr() {
    SinkOptions.Parsed options = pipeline.getOptions().as(SinkOptions.Parsed.class);
    PubsubMessage message = new PubsubMessage(new byte[] {},
        ImmutableMap.of("purpose", "testing error output to stderr"));
    pipeline.apply(Create.of(message)).apply(ErrorOutputType.stderr.write(options));
    pipeline.run();
  }
}
