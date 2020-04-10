package com.mozilla.telemetry.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.internal.IoUtils;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.lang.JoseException;

/**
 * Store Private Keys derived from JSON Web Keys (JWK) decrypted via the GCP Key
 * Management Service (KMS).
 */
public class KeyStore {

  public PrivateKey getKey(String path) {
    ensureKeysLoaded();
    PrivateKey key = keys.get(path);
    return key;
  }

  private final String keysLocation;
  private transient Map<String, PrivateKey> keys;

  // enable this to read from gcs via BeamFileInputStream
  public KeyStore(String keysLocation) {
    this.keysLocation = keysLocation;
  }

  private void loadAllKeys() throws IOException {
    final Map<String, PrivateKey> tempKeys = new HashMap<>();
    InputStream inputStream;
    try {
      inputStream = BeamFileInputStream.open(this.keysLocation);
    } catch (IOException e) {
      throw new IOException("Exception thrown while reading private key");
    }
    byte[] serializedKeyBytes = IoUtils.toByteArray(inputStream);
    String serializedKey = new String(serializedKeyBytes);

    try {
      PublicJsonWebKey key = PublicJsonWebKey.Factory.newPublicJwk(serializedKey);
      tempKeys.put("*", key.getPrivateKey());
    } catch (JoseException e) {
      throw new RuntimeException(e);
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
