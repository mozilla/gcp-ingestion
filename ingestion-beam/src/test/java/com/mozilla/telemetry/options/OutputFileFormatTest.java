/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import static org.junit.Assert.assertEquals;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

public class OutputFileFormatTest {

  @Test
  public void testNullText() {
    assertEquals("", OutputFileFormat.text.encodeSingleMessage(null));
    assertEquals("", OutputFileFormat.text.encodeSingleMessage(new PubsubMessage(null, null)));
  }

  @Test
  public void testNullJson() {
    assertEquals("null", OutputFileFormat.json.encodeSingleMessage(null));
  }

  @Test(expected = UncheckedIOException.class)
  public void testInvalidAttributeMapThrows() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(null, "hi");
    OutputFileFormat.json.encodeSingleMessage(new PubsubMessage(null, attributes));
  }
}
