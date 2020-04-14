package com.mozilla.telemetry.util;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

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
      JSONObject schemaBytes = new JSONObject(new JSONTokener(inputStream));
      schema = SchemaLoader.load(schemaBytes);
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading metadata file");
    }

    JSONArray metadata;
    try (InputStream inputStream = BeamFileInputStream.open(this.metadataLocation)) {
      metadata = new JSONArray(new JSONTokener(inputStream));
      schema.validate(metadata);
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading keystore metadata schema.");
    }

    for (int i = 0; i < metadata.length(); i++) {
      JSONObject entry = metadata.getJSONObject(i);
      String namespace = entry.getString("document_namespace");
      String privateKeyUri = entry.getString("private_key_uri");
      try (InputStream inputStream = BeamFileInputStream.open(privateKeyUri)) {
        String serializedKey = new String(IOUtils.toByteArray(inputStream));
        PublicJsonWebKey key = PublicJsonWebKey.Factory.newPublicJwk(serializedKey);
        tempKeys.put(namespace, key.getPrivateKey());
      } catch (IOException e) {
        // TODO: interpolate specific information from the metadata.
        throw new IOException("Exception thrown while reading key specified by metadata.");
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
