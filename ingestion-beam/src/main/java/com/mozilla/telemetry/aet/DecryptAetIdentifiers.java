package com.mozilla.telemetry.aet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.decoder.ParsePayload;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.Constant.Namespace;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.JweMapping;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import com.mozilla.telemetry.util.KeyStore.KeyNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
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

/**
 * Provides pipeline support for Account Ecosystem Telemetry, decrypting ecosystem_anon_id
 * values to ecosystem_user_id values.
 *
 * <p>Durable AET data stored in BigQuery and available for analysis should contain only
 * ecosystem_user_id values. Conversely, FxA servers should only ever store and see the encrypted
 * ecosystem_anon_id values. This way, someone with access to both BigQuery and FxA servers would
 * still not be able to correlate AET activity with a particular FxA user.
 *
 * <p>This differs from many encryption scenarios in that the encrypted values (ecosystem_anon_id)
 * are themselves sensitive and we must take care not to allow them to flow to BigQuery. The
 * only places where an ecosystem_user_id value and the associated ecosystem_anon_id value can be
 * known together is on the original client device and in this transform in the pipeline.
 * Once we decrypt the value here, we must immediately throw away the ecosystem_anon_id value
 * that was sent to the pipeline.
 *
 * <p>See https://docs.google.com/document/d/1zH3eVVI_28Afg1JXe_McDrW4MTYuWhiJMQR6AQbli8I/edit#heading=h.eo0wl228m5t2
 */
public class DecryptAetIdentifiers extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private transient KeyStore keyStore;
  private transient PipelineMetadataStore pipelineMetadataStore;
  private transient JsonValidator validator;
  private transient Schema telemetryAetSchema;
  private transient Schema structuredAetSchema;

  private final Counter unparsableAetPayload = Metrics.counter(DecryptAetIdentifiers.class,
      "unparsable_aet_payload");
  private final Counter illegalAetUri = Metrics.counter(DecryptAetIdentifiers.class,
      "illegal_aet_uri");
  private final Counter illegalAetPayload = Metrics.counter(DecryptAetIdentifiers.class,
      "illegal_aet_payload");
  private final Counter successTelemetry = Metrics.counter(DecryptAetIdentifiers.class,
      "success_telemetry");
  private final Counter successGlean = Metrics.counter(DecryptAetIdentifiers.class,
      "success_glean");
  private final Counter successStructured = Metrics.counter(DecryptAetIdentifiers.class,
      "success_structured");

  public static final String ACCOUNT_ECOSYSTEM = "account-ecosystem";

  public static DecryptAetIdentifiers of(ValueProvider<String> schemasLocation,
      ValueProvider<String> metadataLocation, ValueProvider<Boolean> kmsEnabled) {
    return new DecryptAetIdentifiers(schemasLocation, metadataLocation, kmsEnabled);
  }

  private DecryptAetIdentifiers(ValueProvider<String> schemasLocation,
      ValueProvider<String> metadataLocation, ValueProvider<Boolean> kmsEnabled) {
    this.schemasLocation = schemasLocation;
    this.metadataLocation = metadataLocation;
    this.kmsEnabled = kmsEnabled;
  }

  /**
   * Base class for all exceptions thrown by this class.
   */
  abstract static class DecryptAetIdentifiersException extends RuntimeException {

    private DecryptAetIdentifiersException() {
    }

    private DecryptAetIdentifiersException(Throwable cause) {
      super(cause);
    }
  }

  public static class UnparsableAetPayloadException extends DecryptAetIdentifiersException {

    public UnparsableAetPayloadException(Exception cause) {
      super(cause);
    }
  }

  public static class IllegalAetUriException extends DecryptAetIdentifiersException {

    public IllegalAetUriException() {
    }
  }

  public static class IllegalAetPayloadException extends DecryptAetIdentifiersException {

    public IllegalAetPayloadException(Exception cause) {
      super(cause);
    }
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class)) //
        .via(new Fn()) //
        .exceptionsInto(TypeDescriptor.of(PubsubMessage.class)) //
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (DecryptAetIdentifiersException e) {
            return FailureMessage.of(DecryptAetIdentifiers.class.getSimpleName(), ee.element(),
                ee.exception());
          }
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
    } else if (anonIdNode.isNull()) {
      return NullNode.getInstance();
    } else {
      throw new IllegalArgumentException(
          "Argument to decrypt must be a TextNode or ArrayNode, but got " + anonIdNode);
    }
  }

  private static void decryptAndReplace(KeyStore keyStore, JsonNode json, List<JweMapping> mappings)
      throws JoseException, KeyNotFoundException {
    for (JweMapping mapping : mappings) {
      JsonNode sourceNode = json.at(mapping.source_field_path());
      if (sourceNode.isMissingNode()) {
        continue;
      }
      JsonNode decrypted = decrypt(keyStore, sourceNode);
      ObjectNode sourceParent = (ObjectNode) json.at(mapping.source_field_path().head());
      sourceParent.remove(mapping.source_field_path().last().getMatchingProperty());
      JsonNode destinationParent = json.at(mapping.decrypted_field_path().head());
      if (destinationParent.isObject()) {
        ((ObjectNode) destinationParent)
            .set(mapping.decrypted_field_path().last().getMatchingProperty(), decrypted);
      } else {
        throw new IllegalArgumentException(
            "Payload is missing parent object for destination field: "
                + mapping.decrypted_field_path());
      }
    }
  }

  private class Fn implements ProcessFunction<PubsubMessage, PubsubMessage> {

    @Override
    public PubsubMessage apply(PubsubMessage message) {
      message = PubsubConstraints.ensureNonNull(message);

      if (pipelineMetadataStore == null) {
        pipelineMetadataStore = PipelineMetadataStore.of(schemasLocation.get(),
            BeamFileInputStream::open);
      }

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
        unparsableAetPayload.inc();
        throw new UnparsableAetPayloadException(e);
      }

      String namespace = message.getAttribute(Attribute.DOCUMENT_NAMESPACE);
      String docType = message.getAttribute(Attribute.DOCUMENT_TYPE);
      HashMap<String, String> attributes = new HashMap<>(message.getAttributeMap());

      try {
        if (!attributes.containsKey(Attribute.DOCUMENT_VERSION)) {
          String version = ParsePayload.getVersionFromTelemetryPayload(json);
          attributes.put(Attribute.DOCUMENT_VERSION, version);
        }
      } catch (SchemaNotFoundException e) {
        throw new IllegalAetPayloadException(e);
      }

      PipelineMetadata meta = pipelineMetadataStore.getSchema(attributes);
      if (meta.jwe_mappings().isEmpty()) {
        illegalAetUri.inc();
        throw new IllegalAetUriException();
      }

      try {
        if (Namespace.TELEMETRY.equals(namespace) //
            && docType != null && docType.startsWith(ACCOUNT_ECOSYSTEM)) {
          validator.validate(telemetryAetSchema, json);
          decryptAndReplace(keyStore, json, meta.jwe_mappings());
          successTelemetry.inc();
        } else if ("firefox-accounts".equals(namespace) //
            && docType != null && docType.startsWith(ACCOUNT_ECOSYSTEM)) {
          validator.validate(structuredAetSchema, json);
          decryptAndReplace(keyStore, json, meta.jwe_mappings());
          successStructured.inc();
        } else {
          // This must be a Glean docType where we don't need extra validation;
          // JWE values are too large to be passed as a normal string value in Glean
          // so we are guaranteed that JWE types must have a registered mapping.
          decryptAndReplace(keyStore, json, meta.jwe_mappings());
          successGlean.inc();
        }
      } catch (IOException | JoseException | ValidationException | KeyNotFoundException
          | IllegalArgumentException e) {
        illegalAetPayload.inc();
        throw new IllegalAetPayloadException(e);
      }

      return new PubsubMessage(Json.asBytes(json), message.getAttributeMap());
    }
  }
}
