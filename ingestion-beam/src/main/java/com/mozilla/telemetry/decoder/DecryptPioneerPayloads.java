package com.mozilla.telemetry.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
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
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
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
import org.jose4j.lang.JoseException;

public class DecryptPioneerPayloads extends
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
  public static final String SCHEMA_NAME = "schemaName";
  public static final String SCHEMA_VERSION = "schemaVersion";
  public static final String PIONEER_ID = "pioneerId";
  public static final String STUDY_NAME = "studyName";
  public static final String DELETION_REQUEST_SCHEMA_NAME = "deletion-request";
  public static final String ENROLLMENT_SCHEMA_NAME = "pioneer-enrollment";

  public static DecryptPioneerPayloads of(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled, ValueProvider<Boolean> decompressPayload) {
    return new DecryptPioneerPayloads(metadataLocation, kmsEnabled, decompressPayload);
  }

  private DecryptPioneerPayloads(ValueProvider<String> metadataLocation,
      ValueProvider<Boolean> kmsEnabled, ValueProvider<Boolean> decompressPayload) {
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

  /**
   * Resolve an invalid namespace due to bug 1674847. The addonId may be sent
   * instead of the schemaNamespace with enrollment pings, so we replace known
   * addonIds with their mappings in the schema repository.
   */
  private static String resolveInvalidNamespace(String schemaNamespace) {
    // https://bugzilla.mozilla.org/show_bug.cgi?id=1674847
    if (schemaNamespace.equals("news.study@princeton.edu")) {
      return "pioneer-citp-news-disinfo";
    }
    return schemaNamespace;
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
      final String namespace = payload.get(SCHEMA_NAMESPACE).asText();
      final String schemaName = payload.get(SCHEMA_NAME).asText();
      final String schemaVersion = payload.get(SCHEMA_VERSION).asText();

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
        if (schemaName.equals(DELETION_REQUEST_SCHEMA_NAME)
            || schemaName.equals(ENROLLMENT_SCHEMA_NAME)) {
          // keep track of these exceptions, which are the cases where we
          // were unable to decrypt the content and push it through anyways
          // to ensure delivery of requests.
          String label = String.format("%s.%s.%s", namespace, schemaName, schemaVersion);
          Metrics.counter(DecryptPioneerPayloads.class, label).inc();
          // The deletion-request and enrollment pings are special cased within
          // the decryption pipeline. The Pioneer client requires a JWE payload to
          // exist before it can be sent. The encrypted data is the empty string
          // encoded with a throwaway key in order to satisfy these requirements.
          // We only need the envelope metadata to shred all client data in a
          // study, so the encrypted data in the deletion-request ping is thrown
          // away. Likewise, the enrollment ping only requires envelope metadata.
          //
          // This changes with the Rally add-on which will contain valid
          // payloads. We continue to send documents through if they do not
          // decrypt properly. Because we are ignoring the exceptions, we do not
          // get a sense of key errors directly in the error stream.
          payloadData = "{}".getBytes(Charsets.UTF_8);
        } else {
          throw e;
        }
      }

      // insert top-level metadata into the payload
      ObjectNode metadata = Json.createObjectNode();
      metadata.put(PIONEER_ID, payload.get(PIONEER_ID).asText());
      metadata.put(STUDY_NAME, payload.get(STUDY_NAME).asText());
      final byte[] merged = NestedMetadata.mergedPayload(payloadData, Json.asBytes(metadata));

      // Redirect messages via attributes
      Map<String, String> attributes = new HashMap<String, String>(message.getAttributeMap());
      attributes.put(Attribute.DOCUMENT_NAMESPACE, resolveInvalidNamespace(namespace));
      attributes.put(Attribute.DOCUMENT_TYPE, schemaName);
      attributes.put(Attribute.DOCUMENT_VERSION, schemaVersion);
      return Collections.singletonList(new PubsubMessage(merged, attributes));
    }
  }
}
