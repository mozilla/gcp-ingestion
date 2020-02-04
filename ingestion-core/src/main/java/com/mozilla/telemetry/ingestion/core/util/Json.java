package com.mozilla.telemetry.ingestion.core.util;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
    return MAPPER.convertValue(objectNode, new TypeReference<Map<String, Object>>() {
    });
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
    TreeNode tree = MAPPER.readTree(data);
    // Check that we have an object, because treeToValue won't
    if (tree == null || !tree.isObject()) {
      throw new IOException("json value is not an object");
    }
    // Return a class instance created from the tree
    return MAPPER.treeToValue(tree, klass);
  }
}
