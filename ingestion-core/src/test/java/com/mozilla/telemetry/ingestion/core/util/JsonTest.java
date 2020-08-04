package com.mozilla.telemetry.ingestion.core.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.UncheckedIOException;
import org.junit.Test;

public class JsonTest {

  private static final JsonNode badNode = new ObjectNode(null) {

    @Override
    public void serialize(JsonGenerator jg, SerializerProvider provider)
        throws JsonProcessingException {
      throw new JsonProcessingException("bad node") {
      };
    }
  };

  @Test
  public void asStringThrowsUnchecked() {
    UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
        () -> Json.asString(badNode));
    assertTrue(thrown.getCause() instanceof JsonProcessingException);
  }

  @Test
  public void asBytesThrowsUnchecked() {
    UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
        () -> Json.asBytes(badNode));
    assertTrue(thrown.getCause() instanceof JsonProcessingException);
  }
}
