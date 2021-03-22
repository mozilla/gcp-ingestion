package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.JweMapping;
import com.mozilla.telemetry.ingestion.core.schema.PipelineMetadataStore.PipelineMetadata;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
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
import org.jose4j.jwe.JsonWebEncryption;
import org.jose4j.lang.JoseException;

public class DecryptJWE extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private final ValueProvider<Boolean> decompressPayload;
  private transient KeyStore keyStore;
  private transient PipelineMetadataStore pipelineMetadataStore;

  public static final String RALLY_ID = "rallyId";
  public static final String STUDY_NAME = "studyName";

  public static DecryptJWE of(ValueProvider<String> metadataLocation,
      ValueProvider<String> schemasLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    return new DecryptJWE(metadataLocation, schemasLocation, kmsEnabled, decompressPayload);
  }

  private DecryptJWE(ValueProvider<String> metadataLocation, ValueProvider<String> schemasLocation,
      ValueProvider<Boolean> kmsEnabled, ValueProvider<Boolean> decompressPayload) {
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
          } catch (IOException | JoseException | KeyNotFoundException
              | IllegalArgumentException e) {
            return FailureMessage.of(DecryptJWE.class.getSimpleName(), //
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

    private void decryptAndReplace(PrivateKey key, JsonNode json, List<JweMapping> mappings)
        throws JoseException, KeyNotFoundException {
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
        JsonNode destinationParent = json.at(mapping.decrypted_field_path().head());
        if (destinationParent.isObject()) {
          ((ObjectNode) destinationParent).set(
              mapping.decrypted_field_path().last().getMatchingProperty(),
              TextNode.valueOf(decrypted.toString()));
        } else {
          throw new IllegalArgumentException(
              "Payload is missing parent object for destination field: "
                  + mapping.decrypted_field_path());
        }
      }
    }

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message)
        throws IOException, JoseException, KeyNotFoundException, IllegalArgumentException {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

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

      PipelineMetadata meta = pipelineMetadataStore.getSchema(attributes);
      if (meta.jwe_mappings().isEmpty()) {
        // Error for lack of better behavior
        throw new RuntimeException(String.format("jwe_mappings missing from schema: %s %s",
            attributes.get(Attribute.DOCUMENT_NAMESPACE), attributes.get(Attribute.DOCUMENT_TYPE)));
      }

      // We do no validation at this stage and leave it for the downstream
      // transform to determine whether the decrypted content is appropriate.
      ObjectNode json = Json.readObjectNode(message.getPayload());

      try {
        // The keying behavior is specific to Rally -- it may be possible to
        // rely on the keyId in the header instead. This behavior should be
        // consistent with how keys are actually allocated.
        PrivateKey key = keyStore.getKeyOrThrow(Attribute.DOCUMENT_NAMESPACE);
        decryptAndReplace(key, json, meta.jwe_mappings());
      } catch (JoseException | KeyNotFoundException | IllegalArgumentException e) {
        throw e;
      }

      return Collections
          .singletonList(new PubsubMessage(Json.asBytes(json), message.getAttributeMap()));
    }
  }
}
