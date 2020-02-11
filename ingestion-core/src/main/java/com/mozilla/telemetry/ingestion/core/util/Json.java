package com.mozilla.telemetry.ingestion.core.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Schema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Json {

  /**
   * A Jackson ObjectMapper pre-configured for use in com.mozilla.telemetry.
   */
  @VisibleForTesting
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final Method SCHEMA_FROM_PB = Arrays.stream(Schema.class.getDeclaredMethods())
      .filter(method -> "fromPb".equals(method.getName())).findFirst().get();

  static {
    SCHEMA_FROM_PB.setAccessible(true);
    MAPPER.registerModule(new JsonOrgModule());
  }

  /**
   * Serialize {@link JsonNode} as a {@link String}.
   */
  public static String asString(JsonNode data) {
    try {
      return MAPPER.writeValueAsString(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Serialize {@code data} as a {@link String}.
   *
   * @exception IOException if data cannot be encoded as json.
   */
  public static String asString(Object data) throws IOException {
    return MAPPER.writeValueAsString(data);
  }

  /**
   * Serialize {@code data} as a {@code byte[]}.
   *
   * @exception IOException if data cannot be encoded as json.
   */
  public static byte[] asBytes(Object data) throws IOException {
    return MAPPER.writeValueAsBytes(data);
  }

  /**
   * Use {@code MAPPER} to convert {@code Map<String, ?>} to {@link ObjectNode}.
   */
  public static ObjectNode asObjectNode(Map<String, ?> map) {
    return MAPPER.valueToTree(map);
  }

  /**
   * Use {@code MAPPER} to convert {@link ObjectNode} to {@code Map<String, Object>}.
   */
  public static Map<String, Object> asMap(ObjectNode objectNode) {
    return convertValue(objectNode, new TypeReference<Map<String, Object>>() {
    });
  }

  /**
   * Use {@code MAPPER} to convert {@link ObjectNode} to an arbitrary class.
   *
   * @throws JsonProcessingException if the conversion is unsuccessful
   */
  public static <T> T convertValue(ObjectNode root, Class<T> klass) throws JsonProcessingException {
    return MAPPER.treeToValue(root, klass);
  }

  /**
   * Use {@code MAPPER} to convert {@link ObjectNode} to an arbitrary class.
   */
  @VisibleForTesting
  public static <T> T convertValue(ObjectNode root, TypeReference<T> typeReference) {
    return MAPPER.convertValue(root, typeReference);
  }

  /**
   * Read a {@link Schema} from a byte array.
   *
   * <p>{@link Schema} does not natively support Jackson deserialization, so we rely on a
   * roundabout method inspired by https://github.com/googleapis/google-cloud-java/issues/2753.
   *
   * @exception IOException if {@code data} does not contain a valid {@link Schema}.
   */
  public static Schema readBigQuerySchema(byte[] data) throws IOException {
    List<TableFieldSchema> fieldsList = (List<TableFieldSchema>) JSON_FACTORY //
        .createJsonParser(new String(data, Charsets.UTF_8)) //
        .parseArray(ArrayList.class, TableFieldSchema.class);
    TableSchema tableSchema = new TableSchema().setFields(fieldsList);

    try {
      return (Schema) SCHEMA_FROM_PB.invoke(null, tableSchema);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read an instance of {@code klass} from a byte array.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  public static <T> T readValue(byte[] data, Class<T> klass) throws IOException {
    // Read data into a tree
    JsonNode tree = MAPPER.readTree(data);
    // Check that we have an object, because treeToValue won't
    if (tree == null || !tree.isObject()) {
      throw new IOException("json value is not an object");
    }
    // Return a class instance created from the tree
    return MAPPER.treeToValue(tree, klass);
  }

  /**
   * Read bytes into a tree of {@link com.fasterxml.jackson.databind.JsonNode}.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  public static ObjectNode readObjectNode(byte[] data) throws IOException {
    // Read data into a tree
    JsonNode root = MAPPER.readTree(data);
    // Check that we have an object, because treeToValue won't
    if (root == null || !root.isObject()) {
      throw new IOException("json value is not an object");
    }
    return (ObjectNode) root;
  }

  public static ObjectNode readObjectNode(String data) throws IOException {
    return readObjectNode(data.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Read bytes into an {@link ArrayNode}.
   *
   * @exception IOException if {@code data} does not contain a valid json object.
   */
  public static ArrayNode readArrayNode(byte[] data) throws IOException {
    // Read data into a tree
    TreeNode root = MAPPER.readTree(data);
    // Check that we have an array, because treeToValue won't
    if (root == null || !root.isArray()) {
      throw new IOException("json value is not an array");
    }
    return (ArrayNode) root;
  }

  /**
   * Return a new, empty {@link ObjectNode}.
   */
  public static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  /**
   * Return a new, empty {@link ArrayNode}.
   */
  public static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }

  /**
   * Return true if {@link ObjectNode} is null or empty.
   */
  public static boolean isNullOrEmpty(ObjectNode node) {
    return node == null || node.size() == 0;
  }

  /**
   * Return true if {@link ObjectNode} is null or empty.
   */
  public static boolean isNullOrEmpty(JsonNode node) {
    return node == null || node.isNull() || node.size() == 0;
  }
}
