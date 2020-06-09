package com.mozilla.telemetry.aet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import com.mozilla.telemetry.util.KeyStore.KeyNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.JoseException;

public class DecryptAetIdentifiers extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private transient KeyStore keyStore;
  private transient JsonValidator validator;
  private transient Schema telemetryAetSchema;
  private transient Schema structuredAetSchema;

  private static final Pattern TELEMETRY_URI_PATTERN = Pattern
      .compile("/submit/telemetry/[^/]+/account-ecosystem.*");
  private static final Pattern STRUCTURED_URI_PATTERN = Pattern
      .compile("/submit/[^/]+/account-ecosystem.*");

  private final Counter nonAetDocType = Metrics.counter(DecryptAetIdentifiers.class,
      "non_aet_doctype");

  public static DecryptAetIdentifiers of(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled) {
    return new DecryptAetIdentifiers(metadataLocation, kmsEnabled);
  }

  private DecryptAetIdentifiers(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled) {
    this.metadataLocation = metadataLocation;
    this.kmsEnabled = kmsEnabled;
  }

  /**
   * Base class for all exceptions thrown by this class.
   */
  abstract static class DecryptAetPayloadException extends RuntimeException {

    private DecryptAetPayloadException(Throwable cause) {
      super(cause);
    }
  }

  public static class UnparsableAetPayloadException extends DecryptAetPayloadException {

    public UnparsableAetPayloadException(Exception cause) {
      super(cause);
    }
  }

  public static class IllegalAetPayloadException extends DecryptAetPayloadException {

    public IllegalAetPayloadException(Exception cause) {
      super(cause);
    }
  }

  /**
   * Return a redacted value if the given node represents a long string.
   *
   * <p>Strings longer than 32 characters might be ecosystem_anon_id values, so we want to make
   * sure they are not propagated to error output.
   */
  private static JsonNode redactedNode(JsonNode node) {
    String value = node.textValue();
    if (value != null && value.length() > 32) {
      return TextNode.valueOf(
          String.format("%s<%d characters redacted>", value.substring(0, 4), value.length() - 4));
    } else {
      return node;
    }
  }

  /**
   * Recursively walk a JSON tree, redacting long string values.
   */
  @VisibleForTesting
  static void sanitizeJsonNode(JsonNode node) {
    if (node.isObject()) {
      List<Entry<String, JsonNode>> fields = Lists.newArrayList(node.fields());
      for (Map.Entry<String, JsonNode> entry : fields) {
        String fieldName = entry.getKey();
        JsonNode value = entry.getValue();
        ((ObjectNode) node).set(fieldName, redactedNode(value));
      }
    }
    if (node.isArray()) {
      List<JsonNode> elements = Lists.newArrayList(node.elements());
      for (int i = 0; i < elements.size(); i++) {
        JsonNode value = elements.get(i);
        if (value.isTextual() && value.asText().length() > 32) {
          ((ArrayNode) node).set(i, redactedNode(value));
        }
      }
    }

    // Recursively sanitize objects and arrays.
    if (node.isContainerNode()) {
      node.elements().forEachRemaining(DecryptAetIdentifiers::sanitizeJsonNode);
    }
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(PubsubMessage.class)) //
        .via(new Fn()) //
        .exceptionsInto(TypeDescriptor.of(PubsubMessage.class)) //
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          byte[] sanitizedPayload;
          try {
            throw ee.exception();
          } catch (UnparsableAetPayloadException e) {
            sanitizedPayload = null;
          } catch (IllegalAetPayloadException e) {
            try {
              ObjectNode json = Json.readObjectNode(ee.element().getPayload());
              sanitizeJsonNode(json);
              sanitizedPayload = Json.asBytes(json);
            } catch (IOException ignore) {
              sanitizedPayload = null;
            }
          }
          return FailureMessage.of(DecryptAetPayloadException.class.getSimpleName(),
              new PubsubMessage(sanitizedPayload, ee.element().getAttributeMap()), ee.exception());
        }));
  }

  /**
   * Decrypt a payload encoded in a compact serialization of JSON Web Encryption (JWE).
   *
   * <p>The payload may be either a single JWE string or an array of values.
   *
   * <p>Assumes that the payload contains a "kid" parameter that can be used to look up a matching
   * private key.
   */
  public static JsonNode decrypt(KeyStore keyStore, JsonNode anonIdNode)
      throws JoseException, KeyNotFoundException {
    if (anonIdNode.isTextual()) {
      String anonId = anonIdNode.textValue();
      JsonWebStructure fromCompact = JsonWebEncryption.fromCompactSerialization(anonId);
      String keyId = fromCompact.getKeyIdHeaderValue();
      PrivateKey key = keyStore.getKeyOrThrow(keyId);
      JsonWebEncryption jwe = new JsonWebEncryption();
      jwe.setKey(key);
      jwe.setContentEncryptionKey(key.getEncoded());
      jwe.setCompactSerialization(anonId);
      return TextNode.valueOf(jwe.getPlaintextString());
    } else if (anonIdNode.isArray()) {
      ArrayNode userIds = Json.createArrayNode();
      for (JsonNode node : anonIdNode) {
        userIds.add(decrypt(keyStore, node));
      }
      return userIds;
    } else {
      throw new IllegalArgumentException(
          "Argument to decrypt must be a TextNode or ArrayNode, but got " + anonIdNode);
    }
  }

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    private void processDesktopTelemetryPayload(ObjectNode json) throws IOException, JoseException {
      validator.validate(telemetryAetSchema, json);
      System.out.println("Go telemetry");
      ObjectNode payload = (ObjectNode) json.path(FieldName.PAYLOAD);
      JsonNode anonIdNode = payload.remove("ecosystemAnonId");
      if (anonIdNode != null) {
        payload.set("ecosystemUserId", decrypt(keyStore, anonIdNode));
      }
      ArrayNode prevAnonIdsNode = (ArrayNode) payload.remove("previousEcosystemAnonIds");
      if (prevAnonIdsNode != null) {
        payload.set("previousEcosystemUserIds", decrypt(keyStore, prevAnonIdsNode));
      }
    }

    private void processStructuredIngestionPayload(ObjectNode json)
        throws IOException, JoseException {
      validator.validate(structuredAetSchema, json);
      JsonNode anonIdNode = json.remove("ecosystem_anon_id");
      if (anonIdNode != null) {
        json.set("ecosystem_user_id", decrypt(keyStore, anonIdNode));
      }
      ArrayNode prevAnonIdsNode = (ArrayNode) json.remove("previous_ecosystem_anon_ids");
      if (prevAnonIdsNode != null) {
        json.set("previous_ecosystem_user_ids", decrypt(keyStore, prevAnonIdsNode));
      }
    }

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);
      String uri = Strings.nullToEmpty(message.getAttribute(Attribute.URI));
      String[] uriParts = uri.split("/");

      if (keyStore == null) {
        // If configured resources aren't available, this throws UncheckedIOException;
        // this is unretryable so we allow it to bubble up and kill the worker and eventually fail
        // the pipeline.
        keyStore = KeyStore.of(metadataLocation.get(), kmsEnabled.get());
      }

      if (validator == null) {
        validator = new JsonValidator();
        try {
          telemetryAetSchema = JSONSchemaStore.readSchema(Resources
              .toByteArray(Resources.getResource("account-ecosystem/telemetry.schema.json")));
          structuredAetSchema = JSONSchemaStore.readSchema(Resources
              .toByteArray(Resources.getResource("account-ecosystem/structured.schema.json")));
        } catch (IOException e) {
          // We let this problem bubble up and kill the worker.
          throw new UncheckedIOException("Unable to load AET JSON schemas", e);
        }
      }

      ObjectNode json;
      try {
        json = Json.readObjectNode(message.getPayload());
      } catch (IOException e) {
        throw new UnparsableAetPayloadException(e);
      }

      // Note that we must work with the raw URI because ParseUri can raise errors that would
      // route payloads containing anon_id values to error output; this transform must be placed
      // before ParseUri so that we can handle redacting anon_id values.
      byte[] normalizedPayload;
      try {
        if (TELEMETRY_URI_PATTERN.matcher(uri).matches()) {
          processDesktopTelemetryPayload(json);
        } else if (false) {
          // Placeholder condition for handling AET payloads coming from Glean-enabled applications;
          // design is evolving in https://bugzilla.mozilla.org/show_bug.cgi?id=1634468
        } else if (STRUCTURED_URI_PATTERN.matcher(uri).matches()) {
          processStructuredIngestionPayload(json);
        } else {
          nonAetDocType.inc();
          return Collections.singletonList(message);
        }
        normalizedPayload = Json.asBytes(json);
      } catch (IOException | JoseException | ValidationException | KeyNotFoundException e) {
        throw new IllegalAetPayloadException(e);
      }

      return Collections
          .singletonList(new PubsubMessage(normalizedPayload, message.getAttributeMap()));
    }

  }
}
