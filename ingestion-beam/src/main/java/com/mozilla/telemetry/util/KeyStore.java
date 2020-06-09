package com.mozilla.telemetry.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.mozilla.telemetry.ingestion.core.schema.JSONSchemaStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.everit.json.schema.Schema;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.lang.JoseException;

/**
 * Store Private Keys derived from JSON Web Keys (JWK) decrypted via the GCP Key
 * Management Service (KMS).
 */
public class KeyStore {

  /**
   * Fetch a key from the store.
   *
   * @param path fully qualified path to the key
   * @return private key for decryption, or null if key is not found
   */
  public PrivateKey getKey(String path) {
    ensureKeysLoaded();
    PrivateKey key = keys.get(path);
    return key;
  }

  /**
   * Fetch a key from the store.
   *
   * @param path fully qualified path to the key
   * @return private key for decryption
   * @throws KeyNotFoundException if key is not found
   */
  public PrivateKey getKeyOrThrow(String path) throws KeyNotFoundException {
    PrivateKey key = getKey(path);
    if (key == null) {
      throw new KeyNotFoundException("No such entry in this KeyStore: " + path);
    }
    return key;
  }

  public static class KeyNotFoundException extends RuntimeException {

    public KeyNotFoundException(String message) {
      super(message);
    }
  }

  private final String metadataLocation;
  private final boolean kmsEnabled;
  private transient Map<String, PrivateKey> keys;

  public static KeyStore of(String metadataLocation, boolean kmsEnabled) {
    return new KeyStore(metadataLocation, kmsEnabled);
  }

  private KeyStore(String metadataLocation, boolean kmsEnabled) {
    // throw immediately if the keyStore is misconfigured
    if (metadataLocation == null) {
      throw new IllegalArgumentException("Metadata location is missing.");
    }
    this.metadataLocation = metadataLocation;
    this.kmsEnabled = kmsEnabled;
  }

  @VisibleForTesting
  int numLoadedKeys() {
    ensureKeysLoaded();
    return keys.size();
  }

  private void loadAllKeys() throws IOException {
    final Map<String, PrivateKey> tempKeys = new HashMap<>();

    Schema schema;
    try {
      byte[] data = Resources.toByteArray(Resources.getResource("keystore-metadata.schema.json"));
      schema = JSONSchemaStore.readSchema(data);
    } catch (IOException e) {
      throw new IOException("Error reading keystore metadata schema file", e);
    }

    // required to validate Jackson objects
    JsonValidator validator = new JsonValidator();

    ArrayNode metadata;
    try (InputStream inputStream = BeamFileInputStream.open(this.metadataLocation)) {
      byte[] data = IOUtils.toByteArray(inputStream);
      metadata = Json.readArrayNode(data);
      validator.validate(schema, metadata);
    } catch (IOException e) {
      throw new IOException("Error reading keystore metadata schema.", e);
    }

    for (JsonNode element : metadata) {
      String privateKeyId = element.get("private_key_id").textValue();
      String privateKeyUri = element.get("private_key_uri").textValue();
      String kmsResourceId = element.get("kms_resource_id").textValue();

      try (InputStream inputStream = BeamFileInputStream.open(privateKeyUri)) {
        byte[] keyData = IOUtils.toByteArray(inputStream);

        PublicJsonWebKey key;
        if (kmsEnabled) {
          try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            DecryptResponse response = client.decrypt(kmsResourceId, ByteString.copyFrom(keyData));
            key = PublicJsonWebKey.Factory.newPublicJwk(response.getPlaintext().toStringUtf8());
          }
        } else {
          key = PublicJsonWebKey.Factory.newPublicJwk(new String(keyData, StandardCharsets.UTF_8));
        }

        tempKeys.put(privateKeyId, key.getPrivateKey());
      } catch (IOException e) {
        throw new IOException("Error reading key specified by metadata.", e);
      } catch (JoseException e) {
        throw new RuntimeException(e);
      }
    }

    keys = tempKeys;
  }

  private void ensureKeysLoaded() {
    if (keys == null) {
      try {
        loadAllKeys();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
