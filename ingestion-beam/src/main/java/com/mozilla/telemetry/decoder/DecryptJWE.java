package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.FieldName;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.ingestion.core.transform.NestedMetadata;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.GzipUtil;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
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

public class DecryptJWE extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private final ValueProvider<Boolean> decompressPayload;
  private transient KeyStore keyStore;
  private transient JsonValidator validator;
  private transient Schema envelopeSchema;

  public static final String ENCRYPTED_DATA = "encryptedData";
  public static final String ENCRYPTION_KEY_ID = "encryptionKeyId";
  public static final String SCHEMA_NAMESPACE = "schemaNamespace";
  public static final String RALLY_ID = "rallyId";
  public static final String STUDY_NAME = "studyName";

  public static DecryptJWE of(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled, ValueProvider<Boolean> decompressPayload) {
    return new DecryptPioneerPayloads(metadataLocation, kmsEnabled, decompressPayload);
  }

  private DecryptJWE(ValueProvider<String> metadataLocation, ValueProvider<Boolean> kmsEnabled,
      ValueProvider<Boolean> decompressPayload) {
    this.metadataLocation = metadataLocation;
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
          } catch (IOException | JoseException | ValidationException e) {
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

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message)
        throws IOException, JoseException, ValidationException {
      message = PubsubConstraints.ensureNonNull(message);

      if (keyStore == null) {
        // If configured resources aren't available, this throws UncheckedIOException;
        // this is unretryable so we allow it to bubble up and kill the worker and eventually fail
        // the pipeline.
        keyStore = KeyStore.of(metadataLocation.get(), kmsEnabled.get());
      }

      if (validator == null) {
        validator = new JsonValidator();
        byte[] data = Resources
            .toByteArray(Resources.getResource("telemetry.pioneer-study.4.schema.json"));
        envelopeSchema = JSONSchemaStore.readSchema(data);
      }

      ObjectNode json = Json.readObjectNode(message.getPayload());
      validator.validate(envelopeSchema, json);
      JsonNode payload = json.get(FieldName.PAYLOAD);

      byte[] payloadData;
      try {
        String encryptionKeyId = payload.get(ENCRYPTION_KEY_ID).asText();
        PrivateKey key = keyStore.getKey(encryptionKeyId);
        if (key == null) {
          // Is this really an IOException?
          throw new IOException(String.format("encryptionKeyId not found: %s", encryptionKeyId));
        }

        final byte[] decrypted = decrypt(key, payload.get(ENCRYPTED_DATA).asText());

        if (decompressPayload.get()) {
          payloadData = GzipUtil.maybeDecompress(decrypted);
        } else {
          // don't bother decompressing
          payloadData = decrypted;
        }
      } catch (IOException | JoseException e) {
        throw e;
      }

      // insert top-level metadata into the payload
      ObjectNode metadata = Json.createObjectNode();
      metadata.put(RALLY_ID, payload.get(RALLY_ID).asText());
      metadata.put(STUDY_NAME, payload.get(STUDY_NAME).asText());
      final byte[] merged = NestedMetadata.mergedPayload(payloadData, Json.asBytes(metadata));

      return Collections.singletonList(new PubsubMessage(merged, attributes));
    }
  }
}
