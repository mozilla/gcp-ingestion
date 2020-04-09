package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.FailureMessage;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.BeamFileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.internal.IoUtils;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.lang.JoseException;

public class PioneerDecryptor extends
    PTransform<PCollection<PubsubMessage>, Result<PCollection<PubsubMessage>, PubsubMessage>> {

  private final ValueProvider<String> keysLocation;
  private transient KeyStore keyStore;

  public static PioneerDecryptor of(ValueProvider keysLocation) {
    return new PioneerDecryptor(keysLocation);
  }

  private PioneerDecryptor(ValueProvider<String> keysLocation) {
    this.keysLocation = keysLocation;
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
          } catch (IOException e) {
            return FailureMessage.of(PioneerDecryptor.class.getSimpleName(), //
                ee.element(), //
                ee.exception());
          }
        }));
  }

  private class KeyStore {

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

  private class Fn implements ProcessFunction<PubsubMessage, Iterable<PubsubMessage>> {

    @Override
    public Iterable<PubsubMessage> apply(PubsubMessage message) throws IOException {
      message = PubsubConstraints.ensureNonNull(message);
      Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

      if (keyStore == null) {
        keyStore = new KeyStore(keysLocation.get());
      }

      byte[] payload = message.getPayload();
      return Collections.singletonList(new PubsubMessage(payload, attributes));
    }
  }
}
