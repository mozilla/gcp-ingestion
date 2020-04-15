package com.mozilla.telemetry.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.api.pathtemplate.ValidationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
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
  private transient Map<String, PrivateKey> keys;

  public static KeyStore of(String metadataLocation) {
    return new KeyStore(metadataLocation);
  }

  private KeyStore(String metadataLocation) {
    this.metadataLocation = metadataLocation;
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
      throw new IOException("Exception thrown while reading metadata file");
    }

    // required to validate Jackson objects
    JsonValidator validator = new JsonValidator();

    ArrayNode metadata;
    try (InputStream inputStream = BeamFileInputStream.open(this.metadataLocation)) {
      byte[] data = IOUtils.toByteArray(inputStream);
      metadata = Json.readArrayNode(data);
      validator.validate(schema, metadata);
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading keystore metadata schema.");
    }

    for (JsonNode element : metadata) {
      String namespace = element.get("document_namespace").textValue();
      String privateKeyUri = element.get("private_key_uri").textValue();
      try (InputStream inputStream = BeamFileInputStream.open(privateKeyUri)) {
        String serializedKey = new String(IOUtils.toByteArray(inputStream));
        PublicJsonWebKey key = PublicJsonWebKey.Factory.newPublicJwk(serializedKey);
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
