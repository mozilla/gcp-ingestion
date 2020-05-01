package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.JsonValidator;
import com.mozilla.telemetry.util.KeyStore;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashMap;
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

public class DecryptPioneerPayloads extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> metadataLocation;
  private final ValueProvider<Boolean> kmsEnabled;
  private transient KeyStore keyStore;
  private transient JsonValidator validator;
  private transient Schema envelopeSchema;

  public static DecryptPioneerPayloads of(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled) {
    return new DecryptPioneerPayloads(metadataLocation, kmsEnabled);
  }

  private DecryptPioneerPayloads(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled) {
    this.metadataLocation = metadataLocation;
    this.kmsEnabled = kmsEnabled;
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
          } catch (IOException | JoseException e) {
            return FailureMessage.of(DecryptPioneerPayloads.class.getSimpleName(), //
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
      JsonNode payload = json.get("payload");

      String encryptionKeyId = payload.get("encryptionKeyId").asText();
      PrivateKey key = keyStore.getKey(encryptionKeyId);
      if (key == null) {
        throw new RuntimeException(String.format("encryptionKeyId not found: %s", encryptionKeyId));
      }

      // NOTE: there could be a method for handling merging metadata like
      // application into the decrypted payload. More serialization and
      // deserialization could be detrimental to performance, and they payload
      // may be gzipped.
      byte[] decrypted = decrypt(key, payload.get("encryptedData").asText());

      // Redirect messages via attributes
      Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());
      attributes.put(Attribute.DOCUMENT_NAMESPACE, payload.get("schemaNamespace").asText());
      attributes.put(Attribute.DOCUMENT_TYPE, payload.get("schemaName").asText());
      attributes.put(Attribute.DOCUMENT_VERSION, payload.get("schemaVersion").asText());
      return Collections.singletonList(new PubsubMessage(decrypted, attributes));
    }
  }
}
