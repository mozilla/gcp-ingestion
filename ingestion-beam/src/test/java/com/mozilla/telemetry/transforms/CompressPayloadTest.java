/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import static org.junit.Assert.assertThat;

import java.util.HashMap;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

public class CompressPayloadTest {

  @Test
  public void testGzipCompress() {
    String text = StringUtils.repeat("Lorem ipsum dolor sit amet ", 100);
    PubsubMessage message = new PubsubMessage(text.getBytes(), new HashMap<>());
    byte[] compressedBytes = CompressPayload.compress(message, Compression.GZIP).getPayload();
    assertThat(ArrayUtils.toObject(compressedBytes), Matchers.arrayWithSize(68));
  }
}
