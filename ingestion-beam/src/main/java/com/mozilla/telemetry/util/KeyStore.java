package com.mozilla.telemetry.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.api.pathtemplate.ValidationException;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
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
   * @return private key for decryption
   */
  public PrivateKey getKey(String path) {
    ensureKeysLoaded();
    PrivateKey key = keys.get(path);
    return key;
  }

  private final String metadataLocation;
  private final boolean kmsEnabled;
  private transient Map<String, PrivateKey> keys;

  public static KeyStore of(String metadataLocation, boolean kmsEnabled) {
    return new KeyStore(metadataLocation, kmsEnabled);
  }

  private KeyStore(String metadataLocation, boolean kmsEnabled) {
    this.metadataLocation = metadataLocation;
    this.kmsEnabled = kmsEnabled;
  }

  @VisibleForTesting
  int numLoadedKeys() {
    ensureKeysLoaded();
    return keys.size();
  }

  private void loadAllKeys() throws IOException, ValidationException {
    final Map<String, PrivateKey> tempKeys = new HashMap<>();

    Schema schema;
    try (InputStream inputStream = Resources.getResource("keystore-metadata.schema.json")
        .openStream()) {
      schema = SchemaLoader.load(Json.readJsonObject(inputStream));
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading metadata file", e);
    }

    // required to validate Jackson objects
    JsonValidator validator = new JsonValidator();

    ArrayNode metadata;
    try (InputStream inputStream = BeamFileInputStream.open(this.metadataLocation)) {
      byte[] data = IOUtils.toByteArray(inputStream);
      metadata = Json.readArrayNode(data);
      validator.validate(schema, metadata);
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading keystore metadata schema.", e);
    }

    for (JsonNode element : metadata) {
      String namespace = element.get("document_namespace").textValue();
      String privateKeyUri = element.get("private_key_uri").textValue();
      String kmsResourceId = element.get("kms_resource_id").textValue();

      try (InputStream inputStream = BeamFileInputStream.open(privateKeyUri)) {
        byte[] keyData = IOUtils.toByteArray(inputStream);

        PublicJsonWebKey key;
        if (kmsEnabled) {
          try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            DecryptResponse response = client.decrypt(kmsResourceId, ByteString.copyFrom(keyData));
            key = PublicJsonWebKey.Factory
                .newPublicJwk(new String(response.getPlaintext().toByteArray()));
          }
        } else {
          key = PublicJsonWebKey.Factory.newPublicJwk(new String(keyData));
        }

        tempKeys.put(namespace, key.getPrivateKey());
      } catch (IOException e) {
        throw new IOException("Exception thrown while reading key specified by metadata.", e);
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
        throw new UncheckedIOException("unexpected error while loading keys", e);
      }
    }
  }
}
