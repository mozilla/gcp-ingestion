package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.JweMapping;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import com.mozilla.telemetry.ingestion.core.transform.NestedMetadata;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import com.mozilla.telemetry.util.KeyStore.KeyNotFoundException;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
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
import org.jose4j.lang.JoseException;

public class DecryptRallyPayloads extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private final ValueProvider<Boolean> decompressPayload;
  private transient KeyStore keyStore;
  private transient PipelineMetadataStore pipelineMetadataStore;
  private transient JsonValidator validator;
  private transient Schema gleanSchema;

  public static final String PIONEER_ID = "pioneerId";
  public static final String RALLY_ID = "rallyId";
  public static final String STUDY_NAME = "studyName";

  public static DecryptRallyPayloads of(ValueProvider<String> metadataLocation,
      ValueProvider<String> schemasLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    return new DecryptRallyPayloads(metadataLocation, schemasLocation, kmsEnabled,
        decompressPayload);
  }

  private DecryptRallyPayloads(ValueProvider<String> metadataLocation,
      ValueProvider<String> schemasLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    this.metadataLocation = metadataLocation;
    this.schemasLocation = schemasLocation;
    this.kmsEnabled = kmsEnabled;
    this.decompressPayload = decompressPayload;
  }

  @Override
  public Result<PCollection<PubsubMessage>, PubsubMessage> expand(
      PCollection<PubsubMessage> messages) {
    return messages.apply(FlatMapElements.into(TypeDescriptor.of(PubsubMessage.class)) //
        .via(new Fn()) //
        .exceptionsInto(TypeDescriptor.of(PubsubMessage.class)) //
        .exceptionsVia((WithFailures.ExceptionElement<PubsubMessage> ee) -> {
          try {
            throw ee.exception();
          } catch (IOException | JoseException | KeyNotFoundException | ValidationException e) {
            return FailureMessage.of(DecryptRallyPayloads.class.getSimpleName(), //
                ee.element(), //
                ee.exception());
          }
        }));
  }

  /**
   * Decrypt a payload encoded in a compact serialization of JSON Web Encryption (JWE).
   */
  public static byte[] decrypt(PrivateKey key, String payload) throws JoseException {
    JsonWebEncryption jwe = new JsonWebEncryption();
    jwe.setKey(key);
    jwe.setContentEncryptionKey(key.getEncoded());
    jwe.setCompactSerialization(payload);
    return jwe.getPlaintextBytes();
  }

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    /** Replace decrypted elements in-place. Encrypted fields are assumed to be objects. */
    private void decryptAndReplace(PrivateKey key, JsonNode json, List<JweMapping> mappings)
        throws IOException, JoseException, KeyNotFoundException, ValidationException {
      for (JweMapping mapping : mappings) {
        JsonNode sourceNode = json.at(mapping.source_field_path());
        if (sourceNode.isMissingNode()) {
          continue;
        }

        final byte[] decryptedData = decrypt(key, sourceNode.asText());
        byte[] decrypted;
        if (decompressPayload.get()) {
          decrypted = GzipUtil.maybeDecompress(decryptedData);
        } else {
          // don't bother decompressing
          decrypted = decryptedData;
        }

        ObjectNode sourceParent = (ObjectNode) json.at(mapping.source_field_path().head());
        sourceParent.remove(mapping.source_field_path().last().getMatchingProperty());
        JsonNode destination = json.at(mapping.decrypted_field_path());

        // Replace the entire object at the destination if it exists
        // https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03
        if (destination.isObject()) {
          // NOTE: we should actually be able to insert any type of JsonNode
          // into the tree, but it's easier to use the existing APIs for
          // parsing out Json data.
          ((ObjectNode) destination).setAll(Json.readObjectNode(decrypted));
        } else {
          // the destination doesn't exist, so let's insert it into an existing
          // parent if it does exist.
          JsonNode destinationParent = json.at(mapping.decrypted_field_path().head());
          if (destinationParent.isObject()) {
            ((ObjectNode) destinationParent).set(
                mapping.decrypted_field_path().last().getMatchingProperty(),
                Json.readObjectNode(decrypted));
          } else {
            throw new RuntimeException("Payload is missing parent object for destination field: "
                + mapping.decrypted_field_path());
          }
        }
      }
    }

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message)
        throws IOException, JoseException, KeyNotFoundException, IllegalArgumentException {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
      final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);

      if (keyStore == null) {
        // If configured resources aren't available, this throws
        // UncheckedIOException; this is unretryable so we allow it to bubble up
        // and kill the worker and eventually fail the pipeline.
        keyStore = KeyStore.of(metadataLocation.get(), kmsEnabled.get());
      }

      if (pipelineMetadataStore == null) {
        pipelineMetadataStore = PipelineMetadataStore.of(schemasLocation.get(),
            BeamFileInputStream::open);
      }

      if (gleanSchema == null || validator == null) {
        JSONSchemaStore schemaStore = JSONSchemaStore.of(schemasLocation.get(),
            BeamFileInputStream::open);
        gleanSchema = schemaStore.getSchema("glean/glean/glean.1.schema.json");
        validator = new JsonValidator();
      }

      final PipelineMetadata meta = pipelineMetadataStore.getSchema(attributes);
      if (meta.jwe_mappings().isEmpty()) {
        // Error for lack of a better behavior
        throw new RuntimeException(String.format("jwe_mappings missing from schema: %s %s",
            namespace, attributes.get(Attribute.DOCUMENT_TYPE)));
      }

      ObjectNode json = Json.readObjectNode(message.getPayload());

      // Decrypt in-place
      try {
        // The keying behavior is specific to Rally -- it may be possible to
        // rely on the keyId in the header instead. This behavior should be
        // consistent with how keys are actually allocated.
        PrivateKey key = keyStore.getKeyOrThrow(namespace);
        decryptAndReplace(key, json, meta.jwe_mappings());
      } catch (JoseException | KeyNotFoundException e) {
        throw e;
      }

      // Ensure our new payload is a glean document
      validator.validate(gleanSchema, json);

      // Ensure that our payload has a uuid field named rally.id, otherwise
      // throw this docuemnt into the error stream.
      final JsonNode rallyId = json.at("/metrics/uuid/rally.id");
      if (rallyId.isMissingNode()) {
        throw new RuntimeException("missing field: #/metrics/uuid/rally.id");
      }

      // Apply the correct identifier based on the namespace. The ingestion
      // system is configured to route rally-* and pioneer-* into the instance
      // of the decoder running this transform.
      ObjectNode metadata = Json.createObjectNode();
      if (namespace.startsWith("rally-")) {
        metadata.put(RALLY_ID, rallyId.asText());
      } else if (namespace.startsWith("pioneer-")) {
        // It's entirely feasible that data is sent an existing pioneer study
        // (e.g. pioneer-core accepts a glean.js ping from the rally-core
        // addon). In these cases, the rally id will be configured to be the
        // pioneer id for legacy reasons.
        metadata.put(PIONEER_ID, rallyId.asText());
      } else {
        // We must have either a rally id or a pioneer id to continue. This
        // would mean that the decoder is misconfigured.
        throw new RuntimeException("unexpected namespace: neither rally-* nor pioneer-*");
      }

      // There is no fixed-concept of a study name in this transform. We'll just
      // insert the document namespace instead.
      metadata.put(STUDY_NAME, namespace);

      // Merge the metadata into the main document so it exists during
      // validation downstream.
      final byte[] merged = NestedMetadata.mergedPayload(Json.asBytes(json),
          Json.asBytes(metadata));

      return Collections.singletonList(new PubsubMessage(merged, message.getAttributeMap()));
    }
  }
}
