package com.mozilla.telemetry.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

public class OutputFileFormatTest {

  private static final byte[] emptyBytes = new byte[] {};

  @Test
  public void testEmptyText() {
    assertEquals("", OutputFileFormat.text.encodeSingleMessage(null));
    assertEquals("",
        OutputFileFormat.text.encodeSingleMessage(new PubsubMessage(emptyBytes, null)));
  }

  @Test
  public void testNullJson() {
    assertEquals("null", OutputFileFormat.json.encodeSingleMessage(null));
  }

  @Test
  public void testInvalidAttributeMapThrows() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(null, "hi");
    assertThrows(UncheckedIOException.class,
        () -> OutputFileFormat.json.encodeSingleMessage(new PubsubMessage(emptyBytes, attributes)));
  }
}
