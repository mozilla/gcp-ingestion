package com.mozilla.telemetry.ingestion.core.util;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
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
  public void asStringCanSucceed() throws JsonProcessingException {
    Json.asString(Json.createObjectNode());
    Json.asString((Object) Json.createObjectNode());
  }

  @Test
  public void asStringCanThrowChecked() {
    assertThrows(JsonProcessingException.class, () -> Json.asString((Object) badNode));
  }

  @Test
  public void asStringCanThrowUnchecked() {
    UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
        () -> Json.asString(badNode));
    assertTrue(thrown.getCause() instanceof JsonProcessingException);
  }

  @Test
  public void asBytesCanSucceed() {
    Json.asBytes(Json.createObjectNode());
  }

  @Test
  public void asBytesCanThrowChecked() {
    assertThrows(JsonProcessingException.class, () -> Json.asBytes((Object) badNode));
  }

  @Test
  public void asBytesCanThrowUnchecked() {
    UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
        () -> Json.asBytes(badNode));
    assertTrue(thrown.getCause() instanceof JsonProcessingException);
  }

  @Test
  public void readBigQuerySchemaCanSucceed() throws IOException {
    Json.readBigQuerySchema("[]".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void readBigQuerySchemaCanThrowInvocationTargetException() {
    // throw an exception because the schema field does not contain a valid type
    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> Json.readBigQuerySchema("[{\"name\":\"\"}]".getBytes(StandardCharsets.UTF_8)));
    assertTrue(thrown.getCause() instanceof InvocationTargetException);
    assertTrue(thrown.getCause().getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void readBigQuerySchemaCanThrowIllegalAccessException() {
    // throw an exception because the method is private
    Json.SCHEMA_FROM_PB.setAccessible(false);
    try {
      RuntimeException thrown = assertThrows(RuntimeException.class,
          () -> Json.readBigQuerySchema("[]".getBytes(StandardCharsets.UTF_8)));
      assertTrue(thrown.getCause() instanceof IllegalAccessException);
    } finally {
      // restore access for subsequent tests
      Json.SCHEMA_FROM_PB.setAccessible(true);
    }
  }

  @Test
  public void readArrayNodeRequiresArray() {
    // MissingNode is not a JSON array
    assertThrows(IOException.class, () -> Json.readArrayNode(new byte[] {}));
    // ObjectNode is not a JSON array
    assertThrows(IOException.class,
        () -> Json.readArrayNode("{}".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void readObjectNodeRequiresObject() {
    // MissingNode is not a JSON object
    assertThrows(IOException.class, () -> Json.readObjectNode(new byte[] {}));
    // ArrayNode is not a JSON object
    assertThrows(IOException.class, () -> Json.readObjectNode("[]"));
  }
}
