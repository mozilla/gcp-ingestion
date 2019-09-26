/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import static org.junit.Assert.assertNull;

import com.mozilla.telemetry.decoder.DecoderOptions.Parsed;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

public class DecoderOptionsTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParsedRedisUri() {
    DecoderOptions options = pipeline.getOptions().as(DecoderOptions.class);
    Parsed parsed = DecoderOptions.parseDecoderOptions(options);
    pipeline.run();
    assertNull(parsed.getParsedRedisUri().get());
  }

}
